package analyzer

import analyzer.expression.functions.eventGroup
import org.apache.spark.sql.catalyst.expressions.{Add, AggregateWindowFunction, AttributeReference, Expression, If, Literal}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.NumericRange

object Chronicler {

  val PARTITION_MASK_SHIFT = 33

  val LIT_CREATED: Column = struct(lit(1).as("encoded"))
  val LIT_REMOVED: Column = struct(lit(2).as("encoded"))
  val LIT_TRANSFORMED: Column = struct(lit(4).as("encoded"))

  val startingMutationId: Long = 1
  val startingMutation = CellParams(0.1f, 0.5f, 0.1f, 0.5f, 0.9f, 0.5f)
  val posRange: NumericRange[Float] = -2.0f to 2.0f by 1.0f


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

  def initialEvents(snapshot: Dataset[Cell]): Dataset[GroupedEvent] = {
    snapshot.
      withColumn("time", lit(Double.NegativeInfinity)).
      withColumn("timeDelta", monotonically_increasing_id().cast(IntegerType)).
      withColumn("eventKind", LIT_CREATED).
      withColumn("eventId", lit(0L)).
      as(Encoders.product[GroupedEvent])
  }

  def groupEventsDF(events: Dataset[Event]): DataFrame /*Dataset[GroupedEvent]*/ = {

    val windowSpec = Window.partitionBy("partitionId").orderBy("timeOrder")
    events.
      withColumn("timeOrder", monotonically_increasing_id()).
      repartitionByRange(col("time")).
      //sortWithinPartitions("timeOrder").
      withColumn("partitionId", spark_partition_id().cast(LongType)).
      withColumn("eventId",
       eventGroup(col("timeDelta")).over(windowSpec) + shiftLeft(col("partitionId"), PARTITION_MASK_SHIFT)
    )
  }
  def groupEvents(events: Dataset[Event]): Dataset[GroupedEvent] = {
    groupEventsDF(events).drop("timeOrder","partitionId").as(Encoders.product[GroupedEvent])
  }

  def computeLinearChronicles(initial: Dataset[Cell], groupedEvents: Dataset[GroupedEvent]): DataFrame = {

    val enumeratedEvents: Dataset[GroupedEvent] = initialEvents(initial) unionByName groupedEvents

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
        col("cellParams").as(Encoders.product[CellParams])
      ).as(Encoders.product[ChronicleEntry])

    chronicles
  }

  def computeChronicles(spark: SparkSession, streamPath: String): Dataset[ChronicleEntry] = {

    val stream = StreamReader.readEventStreamLinesParquet(spark, streamPath)
    val events = StreamReader.toEvents(stream)
    val groupedEvents = groupEvents(events)
    val linearChronicles = computeLinearChronicles(startingSnapshot(spark), groupedEvents).
      checkpoint(eager = false)
    val chronicles = computeChronicles(linearChronicles)

    return chronicles
  }

  def writeLinearChronicles(spark: SparkSession, streamPath: String, outputPrefix: String) = {

    val stream = StreamReader.readEventStreamLinesParquet(spark, streamPath)
    val events = StreamReader.toEvents(stream)

    val groupedEvents = groupEvents(events)
    //spark.sparkContext.setJobGroup("grouped events", "write grouped events")
    //groupedEvents.write.mode(SaveMode.Overwrite).parquet(pathPrefix+"/grouped_events.parquet")

    val linearChronicles = computeLinearChronicles(startingSnapshot(spark), groupedEvents)
    //spark.sparkContext.setJobGroup("linear chronicles", "write linear chronicles")
    //linearChronicles.write.mode(SaveMode.Overwrite).parquet(pathPrefix+"/linear_chronicles.parquet")

  }

  def computeOrReadChronicles(spark: SparkSession, streamPath: String, outputPathPrefix: String): Dataset[ChronicleEntry] =
  {
    import spark.implicits._
    //var chronicles: Dataset[ChronicleEntry] = null
    try{
      spark.read.parquet(outputPathPrefix+"/chronicles.parquet").as[ChronicleEntry]
    }catch {
      case _: AnalysisException =>
        spark.sparkContext.setJobGroup("chronicles", "save chronicles")
        computeChronicles(spark, streamPath).
          //repartition(col("particleId")).
          write.
          mode(SaveMode.Overwrite).
          parquet(outputPathPrefix+"/chronicles.parquet")
        spark.read.parquet(outputPathPrefix + "/chronicles.parquet").as[ChronicleEntry]
    }
  }

  def main(args: Array[String]) {

    if (args.length != 1)
      throw new RuntimeException("no prefix path given")

    val streamPath = args(0)
    val outputPrefix = args(1)

    val spark = SparkSession.builder.
      appName("SimBaD analyzer").
      getOrCreate()

    spark.sparkContext.setCheckpointDir(outputPrefix + "/checkpoints/")

    //computeOrReadChronicles(spark, pathPrefix)
    writeLinearChronicles(spark, streamPath, outputPrefix)
    //scala.io.StdIn.readLine()
  }
}
