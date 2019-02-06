package analyzer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

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
    
    val pathPrefix = args(0);
 
    val spark = SparkSession.builder.
      appName("SimBaD analyzer").
      getOrCreate()

    spark.sparkContext.setCheckpointDir(pathPrefix + "/checkpoints/")

    import spark.implicits._
    
    val chronicleEntries = ChronicleLoader.getOrConvertChronicles(spark, pathPrefix)
  
    spark.sparkContext.setJobGroup("max Time", "computing maximum time")
    val maxTime =  getMaxTime(chronicleEntries);    
    println("MAX TIME %s".format(maxTime));

    spark.sparkContext.setJobGroup("final", "saving final configuration")
    saveCSV( pathPrefix + "/final", 
      Snapshots.getFinal(chronicleEntries),
      true)
 
    spark.sparkContext.setJobGroup("snapshots", "computing snapshots & persist")
    val snapshots = Snapshots.
      getSnapshots(chronicleEntries, maxTime)

    spark.sparkContext.setJobGroup("time stats", "compute & save timestats")
    saveCSV(pathPrefix+"/time_stats", 
      Snapshots.getTimeStats(snapshots), 
      true)    

    spark.sparkContext.setJobGroup("cellTree", "compute CellTree")
    val cellTree = Phylogeny.cellTree(chronicleEntries)

    spark.sparkContext.setJobGroup("mutations", "save mutations Dataframe")
    Phylogeny.mutationTree(cellTree).
      triplets.
      map( t => (t.dstId, t.srcId, t.dstAttr, t.attr ) ).
      toDF("id", "parentId", "mutation", "timeFirst").
      write.
      mode("overwrite").
      parquet(pathPrefix + "/mutationTree.parquet")

    val mutationDS = spark.
      read.
      parquet(pathPrefix + "/mutationTree.parquet").
      as[(Long, Long, Mutation, Double)].
      repartition(100,col("id"))
        
    val mutationTree = Graph(
      mutationDS.map(x => (x._1, x._3)).rdd,
      mutationDS.map(x => Edge(x._2, x._1, x._4)).rdd,
      Phylogeny.noMutation,
      StorageLevel.DISK_ONLY,
      StorageLevel.DISK_ONLY
    )

    spark.sparkContext.setJobGroup("lineage","phylogeny lineage")
    Phylogeny.lineage(mutationTree).
      vertices.
      toDF("id","lineage").
      write.
      mode("overwrite").
      parquet(pathPrefix + "/lineages.parquet")

    val lineages = spark.
      read.
      parquet(pathPrefix + "/lineages.parquet").
      as[(Long,List[Long])].
      repartition(100, col("id"))

    spark.sparkContext.setJobGroup("muller","save muller plot data")
    saveCSV(pathPrefix + "/muller_plot_data", 
      Muller.mullerData(spark, chronicleEntries, lineages, maxTime, 100),
      true);
  }
}