package analyzer

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

object ChronicleLoader{
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
  
  val streamSchema = StructType(Array(
    StructField("position_0", FloatType, false),
    StructField("position_1", FloatType, false),
    StructField("position_2", FloatType, false),
    StructField("density", FloatType, false),
    StructField("next_event_time", FloatType, false),
    StructField("next_event_kind", IntegerType, false),
    StructField("birth_efficiency", FloatType, false),
    StructField("birth_resistance", FloatType, false),
    StructField("lifespan_efficiency", FloatType, false),
    StructField("lifespan_resistance", FloatType, false),
    StructField("success_efficiency", FloatType, false),
    StructField("success_resistance", FloatType, false),
    StructField("mutation_id", LongType, false),
    StructField("birth_rate", FloatType, false),
    StructField("death_rate", FloatType, false),
    StructField("success_probability", FloatType, false),
    StructField("lifespan", FloatType, false),
    StructField("event_time", FloatType, false),
    StructField("event_time_delta", IntegerType, false),
    StructField("event_kind", IntegerType, false)
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

  def loadEntries(spark: SparkSession, path: String) : Dataset[ChronicleEntry] = {
    import spark.implicits._
    loadLines(spark, path).map( line => line.toChronicleEntry );
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