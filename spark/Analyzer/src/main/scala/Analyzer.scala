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
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.stddev
import org.apache.spark.sql.functions.explode

// import analyzer.ChronicleLoader
// import analyzer.ChronicleLine

//    import org.apache.spark.sql.Row

object Analyzer {
  
  
  
  def getMaxTime( chronicles: Dataset[ChronicleLine] ) : Double = {
    chronicles.agg( max("birth_time") ).collect()(0).getDouble(0);
  }
  
  def getMutationTree( spark: SparkSession, chronicles: Dataset[ChronicleLine] ) : DataFrame = {
    
    import spark.implicits._
    
    val uglyTree =  chronicles.
      select("id", "parent_id", "mutation_id").alias("children").
        join(chronicles.select("id","mutation_id").alias("parents"), col("children.parent_id")===col("parents.id"), "left" )

    val tree = uglyTree.map( r => (
      r.getLong(0),
      r.getLong(2),
      if (r.isNullAt(3)) 0 else r.getLong(3),
      if (r.isNullAt(4)) 0 else r.getLong(4)
    )).
      withColumnRenamed("_1", "id").
      withColumnRenamed("_2", "mutation").
      withColumnRenamed("_3","parent").
      withColumnRenamed("_4","parent_mutation")

    val mutationTree = 
      tree.filter( $"mutation" =!= $"parent_mutation").
      dropDuplicates("mutation").
      withColumnRenamed("id","first_particle_id").
      withColumnRenamed("parent_id","first_particle_parent")

    mutationTree;    
  } 
  
  def getTimeStats( chronicles: Dataset[ChronicleLine], maxTime: Double ) : DataFrame = {
    val snapshotsUdf = udf( (t1:Double, t2:Double) => (0d to maxTime by 1).filter( t => t1 < t && t < t2 ) )

    val snapshots = chronicles.withColumn("time_point", explode(snapshotsUdf(col("birth_time"), col("death_time")) ))

    val timeStats = snapshots.groupBy("time_point").agg(
      count(lit(1)).alias("count"), 
      //max( sqrt( $"position_0"*$"position_0" + $"position_1"*$"position_1" + $"position_2"*$"position_2")),
      // means
      avg(col("birth_efficiency")).alias("mean_birth_efficiency"), 
      avg(col("birth_resistance")).alias("mean_birth_resistance"), 
      avg(col("lifespan_efficiency")).alias("mean_lifespan_efficiency"), 
      avg(col("lifespan_resistance")).alias("mean_lifespan_resistance"), 
      avg(col("success_efficiency")).alias("mean_sucdess_efficiency"), 
      avg(col("success_resistance")).alias("mean_success_resistance"),

      // stdev
      stddev(col("birth_efficiency")).alias("stddev_birth_efficiency"), 
      stddev(col("birth_resistance")).alias("stddev_birth_resistance"), 
      stddev(col("lifespan_efficiency")).alias("stddev_lifespan_efficiency"), 
      stddev(col("lifespan_resistance")).alias("stddev_lifespan_resistance"), 
      stddev(col("success_efficiency")).alias("stddev_sucdess_efficiency"), 
      stddev(col("success_resistance")).alias("stddev_success_resistance")
    ).orderBy("time_point")
    
    timeStats;
  }
  
  def getFinal( chronicles: Dataset[ChronicleLine]) : DataFrame = {
    // final
    chronicles.filter( col("death_time") === Double.PositiveInfinity ).
      select(
        col("position_0"), col("position_1"), col("position_2"), 
        col("mutation_id"),
        col("birth_efficiency"), col("birth_resistance"), 
        col("lifespan_efficiency"), col("lifespan_resistance"), 
        col("success_efficiency"), col("success_resistance")
      )
  }
  
  def writeSnapshots( chronicles: Dataset[ChronicleLine], pathPrefix: String, maxTime: Double ) = {
    // snapshots
    for( t <- (0d to maxTime by 1d) ){
      chronicles.
      filter( (col("birth_time") < lit(t) ) && (col("death_time") > lit(t) )  ).
      coalesce(1).
      write.
      format("csv").
      option("delimiter",";").
      option("header", "true").
      mode("overwrite").
      save(pathPrefix + "/snapshots/" + t.toString() )  
    }
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
    
    
    val chronicles = ChronicleLoader.loadLines( spark, pathPrefix + "/chronicles.csv.gz" ).
      persist(StorageLevel.MEMORY_AND_DISK_SER)

    val maxTime =  getMaxTime(chronicles);    
    println("MAX TIME %s".format(maxTime));

    getTimeStats( chronicles, maxTime ).coalesce(1).
      write.
      format("csv").
      option("delimiter",";").
      option("header","true").
      mode("overwrite").
      save(pathPrefix + "/time_stats");
    
    getFinal( chronicles ).
      coalesce(1).
      write.
      format("csv").
      option("delimiter",";").
      option("header","true").
      mode("overwrite").
      save(pathPrefix + "/final")

    /*    
    val mutationTree = getMutationTree( spark, chronicles );
    // save mutationTree
    mutationTree.
      sort("mutation").
      select("mutation", "parent_mutation").
      write.
      format("csv").
      option("delimiter",";").
      option("header", "true").
      mode("overwrite").
      save(pathPrefix + "/mutation_tree/");
    */
  }
}