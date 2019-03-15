package analyzer

import org.apache.spark.storage.StorageLevel

import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions.{
  col, count, lit, max
}

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
    
    val chronicles = ChronicleLoader.getOrConvertChronicles(spark, pathPrefix)
  
    spark.sparkContext.setJobGroup("max Time", "computing maximum time")
    val maxTime =  getMaxTime(chronicles);    
    println("MAX TIME %s".format(maxTime));

    val largeMutations: Dataset[Long] = chronicles.
      groupBy("mutationId").
      agg( count(lit(1)).alias("mutationSize") ).
      filter( $"mutationSize" > 1000 ).
      select($"mutationId".as[Long]).
      persist(StorageLevel.MEMORY_AND_DISK)

    val mutationTree = Phylogeny.getOrComputeMutationTree(spark, pathPrefix, chronicles)      
    val lineages = Phylogeny.getOrComputeLineages(spark, pathPrefix, mutationTree)

    val largeMullerOrder = Muller.mullerOrder( 
      lineages.
        join(largeMutations,"mutationId").
        as[Ancestry]
      ).
      persist(StorageLevel.MEMORY_AND_DISK)

    val statsTmpPath = pathPrefix + "/stats.tmp"
    val snapshotPath = pathPrefix + "/snapshots"
    val mullerTmpPath = pathPrefix + "/muller.tmp"

    for( time <- 0d to maxTime by 1.0d)
    {
      val snapshot = Snapshots.getSnapshot(chronicles, time).
        persist(StorageLevel.MEMORY_AND_DISK)

      Snapshots.getTimeStats(spark, snapshot).
        withColumn("timePoint", lit(time)).as[Double].
        write.
        mode("append").
        parquet(statsTmpPath)

      Muller.mullerPlotSnapshot(snapshot, largeMullerOrder).
        write.
        mode("append").
        parquet(mullerTmpPath)

      snapshot.unpersist()
    }
  }
}