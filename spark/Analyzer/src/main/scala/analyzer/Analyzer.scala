package analyzer

import java.io.{File, PrintWriter}

import scala.collection.mutable.MutableList
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.catalyst.expressions.Coalesce
import org.apache.spark.sql.functions.{abs, broadcast, col, count, first, lit, max}
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object Analyzer {
  def getMaxTime( chronicles: Dataset[ChronicleEntry] ): Double = {
    chronicles.sparkSession.sparkContext.setJobGroup("max Time", "computing maximum time")
    chronicles.agg( max("birthTime") ).head().getDouble(0)
  }

  def readTimePoints(spark: SparkSession, path: String ): Seq[Double] = {
    import spark.implicits._
    spark.read.parquet(path).as[Double].collect()
  }

  def readOrComputeTimePoints(chronicles: Dataset[ChronicleEntry], path: String ): Seq[Double] = {
    val spark = chronicles.sparkSession
    import spark.implicits._
    try{
      readTimePoints(spark, path)
    }catch{
      case _: Exception =>
        val maxTime =  getMaxTime(chronicles)
        val timePoints = (0d until maxTime by 1.0d) :+ maxTime
        saveParquet(path, timePoints.toDS().toDF())
        timePoints
    }
  }

  def saveParquet(path: String, dataFrame: DataFrame, coalesce: Boolean = false): Unit = {
    var df = dataFrame
    if(coalesce)
      df = df.coalesce(1)
    df.write.mode(SaveMode.Overwrite).parquet(path)
  }

  def saveCSV(path: String, dataFrame: DataFrame, coalesce: Boolean ): Unit = {
    var df = dataFrame
    if(coalesce)
      df = df.coalesce(1)
    df.
      write.
      option("delimiter",";").
      option("header",true).
      mode(SaveMode.Overwrite).csv(path)
  }

  def saveCSV[T,S](filePath: String, data: Seq[Array[T]], columnNames: Seq[S]): Unit = {
    val pw = new PrintWriter(new File(filePath))
    pw.println(columnNames.mkString(";"))
    data.foreach( line => pw.println(line.mkString(";")) )
    pw.close()
  }

  def saveCSV[T](filePath: String, data: Seq[T]): Unit = {
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

    if( args.length < 1)
      throw new RuntimeException("no stream path given")
    if (args.length < 2)
      throw new RuntimeException("no output directory given")

    val streamPath = args(0) + "/"
    val outputDirectory = args(1) + "/"

    val cloneSizeThreshold: Long = if(args.length >2) args(2).toLong else 1000


    val spark = SparkSession.builder.
      appName("SimBaD analyzer").
      getOrCreate()

    spark.sparkContext.setCheckpointDir(outputDirectory + "checkpoints/")

    import spark.implicits._

    val timePointsPath = outputDirectory + "time_points.parquet"
    val largeClonesPath = outputDirectory + "large_clones.parquet"
    val cloneSnapshotPath = outputDirectory +"clone_snapshots.parquet"
    val finalSnapshotPath = outputDirectory + "final_snapshot.csv"
    val cloneStatsPath = outputDirectory + "clone_stats.parquet"
    val cloneStatsScalarsPath = outputDirectory + "clone_stats_scalars.parquet"
    val largeMullerOrderPath = outputDirectory + "large_muller_order.parquet"
    val mullerPlotDataPath = outputDirectory + "muller_data.parquet"
    val finalMutationFrequencyPath = outputDirectory + "final_mutation_freq.parquet"
    val cloneCountsPath = outputDirectory + "clone_counts.parquet"
    val largeFinalMutationsPath = outputDirectory + "large_final_mutations.parquet"

    val majorStatsPath = outputDirectory + "major_stats.parquet"
    val majorStatsScalarsPath = outputDirectory +",major_stats_scalars.parquet"

    val noiseStatsPath = outputDirectory + "noise_stats.parquet"
    val noiseStatsScalarsPath = outputDirectory +"noise_stats_scalars.parquet"

    val chronicles = Chronicler.
      computeOrReadChronicles(spark, streamPath, outputDirectory)

    val timePoints = readOrComputeTimePoints(chronicles, timePointsPath)

    val cloneSnapshots = Snapshots.computeOrReadCloneSnapshots(cloneSnapshotPath, chronicles, timePoints, partitionByTime = false)

    val cloneStats = CellStats.readOrCompute(cloneStatsPath, cloneSnapshots)
    CellStats.writeHistograms(outputDirectory, cloneStats.map(_.histograms))
    saveParquet(cloneStatsScalarsPath, cloneStats.map(_.scalarStats).toSeq.toDS().toDF())

    val mutations = Phylogeny.getOrComputeMutationBranches(outputDirectory, chronicles)
    val lineages = Phylogeny.getOrComputeLineages(
      outputDirectory,
      mutations.select($"mutationId", $"parentMutationId").as[MutationTreeLink]
    )

    spark.sparkContext.setJobGroup("muller order", "collect muller order for large mutations")


    val largeClones = Muller.readOrComputeLargeClones(largeClonesPath, chronicles, cloneSizeThreshold)
    val largeMullerOrder = Muller.readOrComputeLargeMullerOrder(largeMullerOrderPath, lineages, largeClones)

    val majorClonesBC = spark.sparkContext.broadcast(largeClones.select("mutationId").as[Long].collect().toSet)

    val majorStats = CellStats.readOrCompute(majorStatsPath, cloneSnapshots.filter( x=> majorClonesBC.value.contains(x.mutationId)))
    CellStats.writeHistograms(outputDirectory+"major_", majorStats.map(_.histograms))
    saveParquet(majorStatsScalarsPath, majorStats.map(_.scalarStats).toSeq.toDS().toDF)

    val noiseStats = CellStats.readOrCompute(noiseStatsPath, cloneSnapshots.filter( x=> !majorClonesBC.value.contains(x.mutationId)))
    CellStats.writeHistograms(outputDirectory+"noise_", noiseStats.map(_.histograms))
    saveParquet(noiseStatsScalarsPath, noiseStats.map(_.scalarStats).toSeq.toDS().toDF)

    majorClonesBC.unpersist()

    Muller.writePlotData(mullerPlotDataPath, cloneSnapshots, largeMullerOrder.map(_.mutationId))

    spark.sparkContext.setJobGroup("final", "save final configuration")
    val finalCellSnapshot = Snapshots.getFinalCells(chronicles)
    saveCSV(finalSnapshotPath, finalCellSnapshot, coalesce = false)

    val finalClones = cloneSnapshots.
      filter( abs(col("timePoint") - timePoints.last)< 0.001 ).
      drop("timePoint").
      as[Clone].
      persist()

    spark.sparkContext.setJobGroup("frequency histogram", "save mutation frequency histogram")
    saveParquet(
      finalMutationFrequencyPath,
      CellStats.computeMutationFrequency(
        finalClones,
        lineages
      ).orderBy("ancestorMutationId").toDF()
    )


    val largeFinalMutations = Phylogeny.getOrComputeMutationCounts(cloneCountsPath, lineages, finalClones, mutations)

    spark.sparkContext.setJobGroup("mutation counts", "save mutation counts")
    saveParquet(largeFinalMutationsPath, largeFinalMutations.filter(col("mutationCount")>=cloneSizeThreshold).toDF())

    finalClones.unpersist()

  }
}
