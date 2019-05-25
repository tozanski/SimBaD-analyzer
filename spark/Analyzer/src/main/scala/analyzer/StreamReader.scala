package analyzer

import analyzer.expression.functions.{partition_id, sequentialGroup}


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id, struct}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession, SaveMode }

object StreamReader {
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

  def readEventStreamLinesParquet(spark: SparkSession, pathPrefix: String): Dataset[StreamLine] = {
    spark.
      read.
      format("parquet").
      schema(streamSchema).
      load(pathPrefix+"/stream.parquet").
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

  def readEvents(spark: SparkSession, pathPrefix: String): Dataset[Event] = {
    val lines = readEventStreamLines(spark, pathPrefix + "/stream.csv.gz")
    toEvents(lines)
  }

  def readEnumeratedEvents(spark: SparkSession, pathPrefix: String): Dataset[EnumeratedEvent] = {
    readEvents(spark, pathPrefix).
      withColumn("timeOrder", monotonically_increasing_id()).
      as(Encoders.product[EnumeratedEvent])
  }


  def createOrReadEnumeratedEvents(spark: SparkSession, pathPrefix: String): Dataset[EnumeratedEvent] = {
    import spark.implicits._
    val outputName = pathPrefix + "/events.parquet"

    var events: Dataset[EnumeratedEvent] = null
    try{
      events = spark.read.parquet(outputName).as[EnumeratedEvent]
    }catch {
      case e: Exception =>
        readEvents(spark, pathPrefix).
          withColumn("timeOrder", monotonically_increasing_id()).
          as(Encoders.product[EnumeratedEvent]).
          write.
          mode(SaveMode.Overwrite).
          parquet(outputName)
        events = spark.read.parquet(outputName).as[EnumeratedEvent]
    }
    events
  }


  def main(args: Array[String]) {

    if( args.length != 1 )
      throw new RuntimeException("no prefix path given")

    //args.foreach( println )
    val pathPrefix = args(0)

    val spark = SparkSession.builder.
      appName("SimBaD stream converter").
      getOrCreate()


    readEventStreamLines(spark, pathPrefix).
      write.
      mode(SaveMode.Overwrite).
      parquet(pathPrefix+"/stream.parquet")


    //createOrReadEnumeratedEvents(spark, pathPrefix)
/*
    readEventStreamLines(spark, pathPrefix + "/stream.csv.gz").
      repartitionByRange(col("event_time")).
      withColumn("partitionId", partition_id()).
      write.
      mode(SaveMode.Overwrite).
      format("csv").
      option("delimiter", ";").
      option("header", true).
      save(pathPrefix+"/partitioned_events.csv")*/

    //scala.io.StdIn.readLine()
  }
}
