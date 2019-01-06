import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SaveMode
//    import org.apache.spark.sql.Row



object Analyzer {
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
  );
  
  def main(args: Array[String]) {
    
    if( args.length != 1 )
      throw new RuntimeException("no prefix path given")
    
    args.foreach( println )
    val pathPrefix = args(0)
 
 
    val conf = new SparkConf().setAppName("SimBaD analyzer")
    val sc = new SparkContext(conf)
 
    val chronicles = sc.
      read.
      format("csv").
      option("positiveInf", "inf").
      option("negativeInf", "-inf").
      option("header","true").
      option("delimiter",";").
      option("mode","DROPMALFORMED").
      schema(chronicleSchema).
      load(pathPrefix + "/chronicles.csv.gz").
      as[ChronicleLine];
    
    val maxTime = chronicles.agg( max("birth_time") ).collect()(0).getDouble(0);  
    
    println("MAX TIME %s".format(maxTime))   
    
    //val chroniclesPath = prefixPath + "/chronicles.csv.gz"
    //val textFile = sc.textFile(chroniclesPath, 4).cache()
    //val numLines = textFile.count()



    //println("There are %s lines".format(numLines))
  }
}