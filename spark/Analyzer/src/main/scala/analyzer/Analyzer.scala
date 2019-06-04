package analyzer

import scala.collection.mutable.MutableList
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.catalyst.expressions.Coalesce
import org.apache.spark.sql.functions.{count, lit, max}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object Analyzer {
  def getMaxTime( chronicles: Dataset[ChronicleEntry] ): Double = {
    chronicles.sparkSession.sparkContext.setJobGroup("max Time", "computing maximum time")
    chronicles.agg( max("birthTime") ).collect()(0).getDouble(0)
  }

  def saveParquet(path: String, dataFrame: DataFrame, coalesce: Boolean){
    var df = dataFrame
    if(coalesce)
      df = df.coalesce(1)
    df.write.mode(SaveMode.Overwrite).parquet(path)
  }

  def saveCSV(path: String, dataFrame: DataFrame, coalesce: Boolean ){
    var df = dataFrame
    if(coalesce)
      df = df.coalesce(1)
    df.
      write.
      option("delimiter",";").
      option("header",true).
      mode(SaveMode.Overwrite).csv(path)
  }


  def notNullableSchema(schema: StructType) : StructType = {
    StructType( schema.map{
      case StructField(n, t, _, m) => t match {
        case subType: StructType => StructField(n, notNullableSchema(subType), nullable = false, m)
        case _ => StructField(n, t, nullable = false, m)
      }
    })
  }

  def notNullableSchema(schema: DataType): DataType = {
    schema match {
      case struct: StructType =>
        StructType( struct.map{
          case StructField(n, t, _, m) => StructField(n, notNullableSchema(t), nullable = false, m)
        })
      case x => x
    }
  }



  def main(args: Array[String]) {

    if( args.length != 1 )
      throw new RuntimeException("no prefix path given")

    val pathPrefix = args(0)

    val spark = SparkSession.builder.
      appName("SimBaD analyzer").
      getOrCreate()

    spark.sparkContext.setCheckpointDir(pathPrefix + "/checkpoints/")

    import spark.implicits._

    val chronicles = Chronicler.
      computeOrReadChronicles(spark, pathPrefix).
      coalesce(2). // debug only
      persist() // debug only


    val maxTime =  getMaxTime(chronicles)
/*
    val largeMutations: Dataset[Long] = chronicles.
      groupBy("mutationId").
      agg( count(lit(1)).alias("mutationSize") ).
      filter( $"mutationSize" > 1000 ).
      select($"mutationId".as[Long]).
      persist(StorageLevel.MEMORY_AND_DISK)*/

    val mutationTree = Phylogeny.getOrComputeMutationTree(spark, pathPrefix, chronicles)
    val lineages = Phylogeny.getOrComputeLineages(spark, pathPrefix, mutationTree)

    /*val largeMullerOrder = Muller.mullerOrder(
      lineages.
        join(largeMutations,"mutationId").
        as[Ancestry]
      ).
      persist(StorageLevel.MEMORY_AND_DISK)*/

    val cellStatsPath = pathPrefix + "/cell_stats.csv"
    val histogramStatsPath = pathPrefix + "/histograms.parquet"
    val cloneStatsPath = pathPrefix + "/clone_stats.csv"
    val mullerTmpPath = pathPrefix + "/muller_data.csv"


    var snapshotStats = MutableList[CellStats]()
    var cellHistograms = MutableList[CellHistogram]()
    var cloneStats = List[CloneStats]()

    for( time <- 0d to maxTime by 1.0d)
    {
      val snapshot = Snapshots.
        getSnapshot(chronicles, time).
        coalesce(2). // for debug only
        persist()

      //snapshotStats += CellStats.collect(snapshot)
      cellHistograms += CellHistogram.collect(snapshot)


/*      Muller.mullerPlotSnapshot(snapshot, largeMullerOrder).
        write.
        mode("append").
        parquet(mullerTmpPath)
*/
      snapshot.unpersist()
    }
    //saveCSV(cellStatsPath, snapshotStats.toDS().toDF(), coalesce=true)
    CellHistogram.writeDF(pathPrefix, cellHistograms.toDS())


    saveParquet(histogramStatsPath, cellHistograms.toDS().toDF(), coalesce = true)
  }
}
