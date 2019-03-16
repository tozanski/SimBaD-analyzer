package analyzer

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Add, AggregateWindowFunction, AttributeReference, Expression, If, IsNotNull, LessThanOrEqual, Literal, RowNumberLike, ScalaUDF, SizeBasedWindowFunction, Subtract}


object Chronicler {
  val LIT_CREATED = struct(lit(1).as("encoded"))
  val LIT_REMOVED = struct(lit(2).as("encoded"))
  val LIT_TRANSFORMED = struct(lit(4).as("encoded"))

  val startingMutationId: Long = 1
  val startingMutation = Mutation(0.1f, 0.5f, 0.1f, 0.5f, 0.9f, 0.5f)
  val posRange = -2.0f to 2.0f by 1.0f

  val streamSchema = StructType(Array(
    StructField("position_0", FloatType, nullable=false),
    StructField("position_1", FloatType, nullable=false),
    StructField("position_2", FloatType, nullable=false),
    StructField("density", FloatType, nullable=false),
    StructField("next_event_time", FloatType, nullable=false),
    StructField("next_event_kind", IntegerType, nullable = false),
    StructField("birth_efficiency", FloatType, nullable=false),
    StructField("birth_resistance", FloatType, nullable=false),
    StructField("lifespan_efficiency", FloatType, nullable = false),
    StructField("lifespan_resistance", FloatType, nullable=false),
    StructField("success_efficiency", FloatType, nullable=false),
    StructField("success_resistance", FloatType, nullable=false),
    StructField("mutation_id", LongType, nullable=false),
    StructField("birth_rate", FloatType, nullable=false),
    StructField("death_rate", FloatType, nullable=false),
    StructField("success_probability", FloatType, nullable=false),
    StructField("lifespan", FloatType, nullable=false),
    StructField("event_time", DoubleType, nullable=false),
    StructField("event_time_delta", IntegerType, nullable=false),
    StructField("event_kind", IntegerType, nullable=false)
  ))

  case class StreamLine(
                         position_0: Float,
                         position_1: Float,
                         position_2: Float,
                         density: Float,
                         next_event_time: Double,
                         next_event_kind: Int,
                         birth_efficiency: Float,
                         birth_resistance: Float,
                         lifespan_efficiency: Float,
                         lifespan_resistance: Float,
                         success_efficiency: Float,
                         success_resistance: Float,
                         mutation_id: Long,
                         birth_rate: Float,
                         death_rate: Float,
                         success_probability: Float,
                         lifespan: Float,
                         event_time: Double,
                         event_time_delta: Int,
                         event_kind: Int
                       )


  def startingSnapshot(spark: SparkSession): Dataset[Cell] = {
    import spark.implicits._

    val starting = for (x <- posRange; y <- posRange; z <- posRange) yield
      Cell(
        Position(x, y, z),
        startingMutationId,
        startingMutation
      )
    starting.toDS
  }

  def readEventStreamLines(spark: SparkSession, path: String): Dataset[StreamLine] = {
    spark.
      read.
      format("csv").
      option("positiveInf", "inf").
      option("negativeInf", "-inf").
      option("header", "true").
      option("delimiter", ";").
      option("mode", "DROPMALFORMED").
      schema(streamSchema).
      load(path).
      as(Encoders.product[StreamLine])
  }

  def toEvents(lines: Dataset[StreamLine]): Dataset[Event] = {
    lines.select(
      struct(
        col("event_time").as(Encoders.DOUBLE).as("time"),
        col("event_time_delta").as(Encoders.INT).as("timeDelta"),
        struct(
          col("event_kind").as(Encoders.INT).as("encoded")
        ).as(Encoders.product[EventKind]).as("eventKind"),
        struct(
          col("position_0").as(Encoders.FLOAT).alias("x"),
          col("position_1").as(Encoders.FLOAT).alias("y"),
          col("position_2").as(Encoders.FLOAT).alias("z")
        ).as("position").as(Encoders.product[Position]),
        col("mutation_id").as(Encoders.LONG).as("mutationId"),
        struct(
          col("birth_efficiency").as(Encoders.FLOAT).alias("birthEfficiency"),
          col("birth_resistance").as(Encoders.FLOAT).alias("birthResistance"),
          col("lifespan_efficiency").as(Encoders.FLOAT).alias("lifespanEfficiency"),
          col("lifespan_resistance").as(Encoders.FLOAT).alias("lifespanResistance"),
          col("success_efficiency").as(Encoders.FLOAT).alias("successEfficiency"),
          col("success_resistance").as(Encoders.FLOAT).alias("successResistance")
        ).as("mutation").as(Encoders.product[Mutation])
      ).as(Encoders.product[Event])
    )
  }
  def withExpr(expr: Expression): Column = new Column(expr)

  case class GroupUDWF(marker: Expression) extends AggregateWindowFunction {
    self: Product =>
    def this() = this(Literal(0))

    override def children: Seq[Expression] = marker :: Nil

    override def dataType: DataType = LongType

    protected val zero = Literal(0L)
    protected val one = Literal(1L)

    protected val currentGroup = AttributeReference("currentGroup", LongType, nullable = true)()

    override val aggBufferAttributes: Seq[AttributeReference] = currentGroup :: Nil

    override val initialValues: Seq[Expression] = zero :: Nil
    override val updateExpressions: Seq[Expression] = If(marker, Add(currentGroup, one), currentGroup) :: Nil

    override val evaluateExpression: Expression = aggBufferAttributes(0)

    override def prettyName: String = "sequential_group"
  }

  def sequentialGroup(marker: Column): Column = withExpr {
    GroupUDWF(marker.expr)
  }

  def groupEvents(events: Dataset[Event], singlePartition: Boolean): Dataset[GrouppedEvent] = {

    val window =
      if (singlePartition)
        Window.orderBy("timeOrder")
      else
        Window.partitionBy("time").orderBy("timeOrder")

    events.
      withColumn("timeOrder", monotonically_increasing_id()).
      withColumn("eventId", sequentialGroup(col("timeDelta")===0) over window).
      drop("timeOrder").
      as(Encoders.product[GrouppedEvent])
  }

  def readEvents(spark: SparkSession, pathPrefix: String): Dataset[Event] = {
    val lines = readEventStreamLines(spark, pathPrefix + "/stream.csv.gz")
    toEvents(lines)
  }

  def computeLinearChronicles(initial: Dataset[Cell], grouppedEvents: Dataset[GrouppedEvent]): DataFrame = {

    val initialEvents: Dataset[GrouppedEvent] = initial.
      withColumn("time", lit(Double.NegativeInfinity)).
      withColumn("timeDelta", monotonically_increasing_id().cast(IntegerType)).
      withColumn("eventKind", LIT_CREATED).
      withColumn("eventId", lit(0L)).
      as(Encoders.product[GrouppedEvent])

    val enumeratedEvents: Dataset[GrouppedEvent] = initialEvents unionByName grouppedEvents

    val linearChronicles = enumeratedEvents.
      repartition(256, col("position")).
      withColumn("particleId", monotonically_increasing_id()+1).
      withColumn("parentId",
        lag("particleId", 1, null)
          over Window.partitionBy("position").orderBy("eventId")).
      filter(col("eventKind") =!= LIT_REMOVED).
      withColumn("deathTime",
        lead("time", 1, Double.PositiveInfinity)
          over Window.partitionBy("position").orderBy("eventId")).
      withColumnRenamed("time","birthTime")

    linearChronicles
  }

  def computeChronicles(linearChronicles: DataFrame): Dataset[ChronicleEntry] = {

    val offspring = linearChronicles.
      filter(!isnull(col("parentId"))).
      alias("offspring")

    val settlers = linearChronicles.
      filter(isnull(col("parentId"))).
      drop("parentId").
      alias("settlers")

    val resolvedSettlers = offspring.
      select("eventId", "parentId").
      join(settlers, Seq("eventId"), "RIGHT")

    val chronicles = (resolvedSettlers unionByName offspring).
      select(
        col("particleId").as(Encoders.LONG),
        coalesce(col("parentId"), lit(0L)).as("parentId").as(Encoders.LONG),
        col("birthTime").as(Encoders.DOUBLE),
        col("deathTime").as(Encoders.DOUBLE),
        col("position").as(Encoders.product[Position]),
        col("mutationId").as(Encoders.LONG),
        col("mutation").as(Encoders.product[Mutation])
      ).as(Encoders.product[ChronicleEntry])

    chronicles
  }

  def main(args: Array[String]) {

    if (args.length != 1)
      throw new RuntimeException("no prefix path given")

    val pathPrefix = args(0)

    val spark = SparkSession.builder.
      appName("SimBaD analyzer").
      getOrCreate()

    spark.sparkContext.setCheckpointDir(pathPrefix + "/checkpoints/")

    val events = readEvents(spark, pathPrefix)

    val grouppedEvents = groupEvents(events, true)
    val initialSnapshot: Dataset[Cell] = startingSnapshot(spark)

    val linearChronicles = computeLinearChronicles(initialSnapshot, grouppedEvents)

    linearChronicles.
      write.
      mode("overwrite").
      parquet(pathPrefix+"/linearChronicles.parquet")
  }
}