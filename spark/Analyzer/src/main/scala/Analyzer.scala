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

import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.first

object Analyzer {
  def getMaxTime( chronicles: Dataset[ChronicleEntry] ): Double = {
    chronicles.agg( max("birthTime") ).collect()(0).getDouble(0);
  }
  
  def main(args: Array[String]) {
    
    if( args.length != 1 )
      throw new RuntimeException("no prefix path given");
    
    args.foreach( println );
    val pathPrefix = args(0);
 
 
    val conf = new SparkConf().setAppName("SimBaD analyzer");
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    val spark = sqlContext.sparkSession;
    import spark.implicits._
    
    var chronicleEntries: Dataset[ChronicleEntry] = null;

    try{
      chronicleEntries = spark.
        read.
        parquet(pathPrefix + "/chronicles.parquet").
        as[ChronicleEntry]
    }catch{
      case e: Exception => {
        ChronicleLoader.loadEntries( spark, pathPrefix + "/chronicles.csv.gz" ).
          write.
          parquet(pathPrefix+ "/chronicles.parquet")
        chronicleEntries = spark.
          read.
          parquet(pathPrefix + "/chronicles.parquet").
          as[ChronicleEntry]
      }
    }
    //chronicleEntries = chronicleEntries.
    //  repartitionByRange(1000, $"particleId").
    //  persist(StorageLevel.DISK_ONLY)
  

    val maxTime =  getMaxTime(chronicleEntries);    
    println("MAX TIME %s".format(maxTime));

    val snapshots = Snapshots.getSnapshots(chronicleEntries, maxTime );

    Snapshots.getTimeStats(snapshots).
      coalesce(1).
      write.
      format("csv").
      option("delimiter",";").
      option("header","true").
      mode("overwrite").
      save(pathPrefix + "/time_stats");
    
    Snapshots.getFinal(chronicleEntries).
      coalesce(1).
      write.
      format("csv").
      option("delimiter",";").
      option("header","true").
      mode("overwrite").
      save(pathPrefix + "/final")


  }
}