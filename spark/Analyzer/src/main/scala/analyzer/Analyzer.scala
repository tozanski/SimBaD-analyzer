package analyzer

import java.io.{File, PrintWriter}

import scala.collection.mutable.MutableList
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.catalyst.expressions.Coalesce
import org.apache.spark.sql.functions.{broadcast, col, count, first, lit, max}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object Analyzer {
  def getMaxTime( chronicles: Dataset[ChronicleEntry] ): Double = {
    chronicles.sparkSession.sparkContext.setJobGroup("max Time", "computing maximum time")
    chronicles.agg( max("birthTime") ).head().getDouble(0)
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

  def saveCSV[T,S](filePath: String, data: Seq[Array[T]], columnNames: Seq[S]) = {
    val pw = new PrintWriter(new File(filePath))
    pw.println(columnNames.mkString(";"))
    data.foreach( line => pw.println(line.mkString(";")) )
    pw.close()
  }

  def saveCSV[T](filePath: String, data: Seq[T]) = {
    val pw = new PrintWriter(new File(filePath))
    data.foreach( datum => pw.println(datum))
    pw.close()
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

    val finalSnapshotPath = pathPrefix + "/final_snapshot.csv"
    val cloneStatsPath = pathPrefix + "/clone_stats.csv"
    val mullerPlotDataPath = pathPrefix + "/muller_data.csv"
    val finalMutationFrequencyPath = pathPrefix + "/final_mutation_freq.csv"

    val chronicles = Chronicler.
      computeOrReadChronicles(spark, pathPrefix)
      //.coalesce(2). // debug only
      //persist() // debug only

    val maxTime =  getMaxTime(chronicles)
    //val maxTime = 20.0 // debug only

    val timePoints = (0d until maxTime by 1.0d) :+ maxTime
    saveCSV(pathPrefix + "/time_points.csv", timePoints)

    val cloneSnapshots = Snapshots.computeOrReadCloneSnapshots(pathPrefix, chronicles, timePoints, partitionByTime = false)

    val cloneStats = CloneStats.collect(cloneSnapshots)
    CloneStats.writeHistograms(pathPrefix, cloneStats.map(_.histograms))
    saveCSV(cloneStatsPath, cloneStats.map(_.scalarStats).toSeq.toDS().toDF(), coalesce = true)

    val largeMutations: Dataset[(Long, Mutation)] = chronicles.
      groupBy("mutationId").
      agg(
        count(lit(1)).alias("mutationSize"),
        first(col("mutation")).alias("mutation")
      ).
      filter( $"mutationSize" > 1000 ).
      select($"mutationId".as[Long], $"mutation".as[Mutation]).
      cache()
    spark.sparkContext.setJobGroup("large mutations", "count large mutations")
    println("Large mutation count" + largeMutations.count())

    val mutationTree = Phylogeny.getOrComputeMutationTree(spark, pathPrefix, chronicles)
    val lineages = Phylogeny.getOrComputeLineages(spark, pathPrefix, mutationTree)

    spark.sparkContext.setJobGroup("muller order", "collect muller order for large mutations")
    val largeMullerOrder: Array[MutationOrder] = Muller.mullerOrder(
      lineages.
        join(broadcast(largeMutations.select("mutationId")),"mutationId").
        as[Ancestry]
      ).
      orderBy("ordering").
      collect()

    spark.sparkContext.setJobGroup("large mutations", "save large mutations")
    saveCSV(pathPrefix+"/large_mutations.csv",
      largeMutations.join(broadcast(largeMullerOrder.toSeq.toDS()), "mutationId").
        orderBy("ordering").
        select(
          col("mutationId"),
          col("mutation.birthEfficiency"),
          col("mutation.birthResistance"),
          col("mutation.lifespanEfficiency"),
          col("mutation.lifespanResistance"),
          col("mutation.successEfficiency"),
          col("mutation.successResistance")
        ),
      coalesce=true)

    Muller.writePlotData(mullerPlotDataPath, cloneSnapshots, largeMullerOrder.map(_.mutationId))


    spark.sparkContext.setJobGroup("final", "save final configuration")
    val finalCellSnapshot = Snapshots.getFinalCells(chronicles)
    saveCSV(finalSnapshotPath, finalCellSnapshot, coalesce = false)

    spark.sparkContext.setJobGroup("frequency histogram", "save mutation frequency histogram")
    saveCSV(
      finalMutationFrequencyPath,
      CloneStats.computeMutationFrequency(
        cloneSnapshots.filter(col("timePoint") === maxTime).as[Clone],
        lineages
      ).orderBy("ancestorMutationId").toDF(),
      coalesce = true)
  }
}
