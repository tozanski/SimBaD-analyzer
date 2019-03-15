package analyzer

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{
  col, struct
}
import org.apache.spark.sql.{Encoder, Encoders}

object ChronicleLoader{

  case class ChronicleLine(
    id: Long,
    parent_id: Long,
    birth_time: Double,
    death_time: Double,
    position_0: Float,
    position_1: Float,
    position_2: Float,
    mutation_id: Long,
    birth_efficiency: Float,
    birth_resistance: Float,
    lifespan_efficiency: Float,
    lifespan_resistance: Float,
    success_efficiency: Float,
    success_resistance: Float
  )

  val chronicleSchema = StructType(Array(
    StructField("id", LongType, false),
    StructField("parent_id", LongType, false),
    StructField("birth_time", DoubleType, false),
    StructField("death_time", DoubleType, false),
    StructField("position_0", FloatType, false),
    StructField("position_1", FloatType, false),
    StructField("position_2", FloatType, false),
    StructField("mutation_id", LongType, false),
    StructField("birth_efficiency", FloatType, false),
    StructField("birth_resistance", FloatType, false),
    StructField("lifespan_efficiency", FloatType, false),
    StructField("lifespan_resistance", FloatType, false),
    StructField("success_efficiency", FloatType, false),
    StructField("success_resistance", FloatType, false)
  ));

  def loadLines(spark: SparkSession, path: String) : Dataset[ChronicleLine] = {
    import spark.implicits._
    
    spark.
      read.
      format("csv").
      option("positiveInf", "inf").
      option("negativeInf", "-inf").
      option("header","true").
      option("delimiter",";").
      option("mode","DROPMALFORMED").
      schema(chronicleSchema).
      load(path).
      as[ChronicleLine];
  }
  def toEntries( lines: Dataset[ChronicleLine] ): Dataset[ChronicleEntry] = {
    lines.select( 
      struct(
        col("id").as(Encoders.LONG).as("particleId"),
        col("parent_id").as(Encoders.LONG).as("parentId"),
        col("birth_time").as(Encoders.DOUBLE).as("birthTime"),
        col("death_time").as(Encoders.DOUBLE).as("deathTime"),
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
      ).as(Encoders.product[ChronicleEntry])
    )    
  }

  def loadEntries(spark: SparkSession, path: String) : Dataset[ChronicleEntry] = {
    toEntries(loadLines(spark, path))
  }

  def getOrConvertChronicles(spark: SparkSession, pathPrefix: String ): Dataset[ChronicleEntry] = {
    import spark.implicits._

    var chronicleEntries: Dataset[ChronicleEntry] = null;
    try{
      chronicleEntries = spark.
        read.
        parquet(pathPrefix + "/chronicles.parquet").
        as[ChronicleEntry]
    }catch{
      case e: Exception => {
        loadEntries( spark, pathPrefix + "/chronicles.csv.gz" ).
          write.
          parquet(pathPrefix+ "/chronicles.parquet")
        chronicleEntries = spark.
          read.
          parquet(pathPrefix + "/chronicles.parquet").
          as[ChronicleEntry]
      }
    }
    return chronicleEntries 
  }
}