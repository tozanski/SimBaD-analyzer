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

  def saveCSV( path: String, dataframe: DataFrame, coalesce: Boolean ){
    var df = dataframe;
    if(coalesce)
      df = df.coalesce(1);
    df.
      write.
      format("csv").
      option("delimiter",";").
      option("header",true).
      mode("overwrite").
      save(path)
  }
  
  def main(args: Array[String]) {
    
    if( args.length != 1 )
      throw new RuntimeException("no prefix path given");
    
    args.foreach( println );
    val pathPrefix = args(0);
 
    val spark = SparkSession.builder.
      appName("SimBaD analyzer").
      getOrCreate()

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

    saveCSV( pathPrefix + "/final", 
      Snapshots.getFinal(chronicleEntries),
      true)
 
    val snapshots = Snapshots.getSnapshots(chronicleEntries, maxTime );

    saveCSV(pathPrefix+"/time_stats", 
      Snapshots.getTimeStats(snapshots), 
      true)    

    val cellTree = Phylogeny.cellTree(chronicleEntries);
    val mutationTree = Phylogeny.mutationTree(cellTree).persist()
    val lineageTree = Phylogeny.lineage(mutationTree);

    saveCSV(pathPrefix + "/muller_plot_data", 
      Muller.mullerData(spark, snapshots, lineageTree),
      true);
  }
}