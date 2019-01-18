package analyzer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.storage.StorageLevel

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

object Converter {
  
  def main(args: Array[String]) {
    
    if( args.length != 1 )
      throw new RuntimeException("no prefix path given");
    
    args.foreach( println );
    val pathPrefix = args(0);
 
 
    val conf = new SparkConf().setAppName("SimBaD chronicles converter");
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    val spark = sqlContext.sparkSession;
    import spark.implicits._
    
    ChronicleLoader.loadEntries( spark, pathPrefix + "/chronicles.csv.gz" ).
      write.
      format("parquet").
      save(pathPrefix+"/chronicles.parquet")
  }
}
