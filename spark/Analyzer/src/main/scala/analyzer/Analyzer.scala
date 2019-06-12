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

    val chronicles = Chronicler.
      computeOrReadChronicles(spark, pathPrefix)
      //.coalesce(2). // debug only
      //persist() // debug only

    val maxTime =  getMaxTime(chronicles)
    //val maxTime = 5.0 // debug only

    val largeMutations: Dataset[(Long, Mutation)] = chronicles.
      groupBy("mutationId").
      agg(
        count(lit(1)).alias("mutationSize"),
        first(col("mutation")).alias("mutation")
      ).
      filter( $"mutationSize" > 1000 ).
      select($"mutationId".as[Long], $"mutation".as[Mutation]).
      cache()

    val mutationTree = Phylogeny.getOrComputeMutationTree(spark, pathPrefix, chronicles)
    val lineages = Phylogeny.getOrComputeLineages(spark, pathPrefix, mutationTree)

    val largeMullerOrder: Dataset[MutationOrder] = Muller.mullerOrder(
      lineages.
        join(broadcast(largeMutations.select("mutationId")),"mutationId").
        as[Ancestry]
      ).
      coalesce(1).
      persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    spark.sparkContext.setJobGroup("large mutations", "save large mutations")
    saveCSV(pathPrefix+"/large_mutations.csv",
      largeMutations.join(broadcast(largeMullerOrder), "mutationId").
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


    spark.sparkContext.setJobGroup("large mutation names", "collect large mutations")
    val mullerMutationNames =
      "noise" +: largeMullerOrder.select(col("mutationId").as[Long]).collect().map("mutation_" + _.toString)


    val cellStatsPath = pathPrefix + "/cell_stats.csv"
    val histogramStatsPath = pathPrefix + "/histograms.parquet"
    val cloneStatsPath = pathPrefix + "/clone_stats.csv"
    val mullerPlotPath = pathPrefix + "/muller_data.csv"
    val finalMutationFrequencyPath = pathPrefix + "/final_mutation_freq.csv"


    var snapshotStats = MutableList[CellStats]()
    var cellHistograms = MutableList[CellHistogram]()
    var cloneStats = MutableList[CloneStats]()
    var mullerPlotData = MutableList[Array[Long]]()

    var snapshot: Dataset[Cell] = null
    var clones: Dataset[Clone] = null

    val timePoints = (0d until maxTime by 1.0d) :+ maxTime
    saveCSV(pathPrefix + "/time_points.csv", timePoints)

    for( time <- timePoints)
    {
      snapshot = Snapshots.
        getSnapshot(chronicles, time).
        coalesce(64). // for debug only
        persist()

      snapshotStats += CellStats.collect(snapshot)
      cellHistograms += CellHistogram.collect(snapshot)

      clones = Snapshots.getClones(snapshot).coalesce(32).persist()
      cloneStats += CloneStats.collect(clones)

      mullerPlotData += Muller.collect(clones, largeMullerOrder)

      snapshot.unpersist()
      clones.unpersist()
    }
    saveCSV(cellStatsPath, snapshotStats.toDS().toDF(), coalesce=true)
    saveCSV(cloneStatsPath, cloneStats.toDF(), coalesce=true)

    saveCSV(
      finalMutationFrequencyPath,
      CloneStats.computeMutationFrequency(clones, lineages).orderBy("ancestorMutationId").toDF(),
      coalesce = true)

    saveCSV(mullerPlotPath, mullerPlotData, mullerMutationNames)
  }
}
