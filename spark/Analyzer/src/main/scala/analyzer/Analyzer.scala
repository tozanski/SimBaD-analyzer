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
    val cloneStatsPathCSV = outputDirectory + "clone_stats_scalars.parquet"
    val largeMullerOrderPath = outputDirectory + "large_muller_order.parquet"
    val mullerPlotDataPath = outputDirectory + "muller_data.parquet"
    val finalMutationFrequencyPath = outputDirectory + "final_mutation_freq.parquet"
    val cloneCountsPath = outputDirectory + "clone_counts.parquet"
    val largeFinalMutationsPath = outputDirectory + "large_final_mutations.parquet"

    val chronicles = Chronicler.
      computeOrReadChronicles(spark, streamPath, outputDirectory)

    val timePoints = readOrComputeTimePoints(chronicles, timePointsPath)

    val cloneSnapshots = Snapshots.computeOrReadCloneSnapshots(cloneSnapshotPath, chronicles, timePoints, partitionByTime = false)

    val cloneStats = CellStats.readOrCompute(cloneStatsPath, cloneSnapshots)
    CellStats.writeHistograms(outputDirectory, cloneStats.map(_.histograms))
    saveParquet(cloneStatsPathCSV, cloneStats.map(_.scalarStats).toSeq.toDS().toDF())

    /*
    val largeClones: Dataset[(Long, CellParams)] = chronicles.
      groupBy("mutationId").
      agg(
        count(lit(1)).alias("mutationSize"),
        first(col("cellParams")).alias("cellParams")
      ).
      filter( $"mutationSize" > 1000 ).
      select($"mutationId".as[Long], $"cellParams".as[CellParams]).
      cache()*/
    val largeClones = Muller.readOrComputeLargeClones(largeClonesPath, chronicles)
    /*
    spark.sparkContext.setJobGroup("large clones", "count large clones")
    println("Large clones count" + largeClones.count())
    */


    val mutations = Phylogeny.getOrComputeMutationBranches(outputDirectory, chronicles)
    val lineages = Phylogeny.getOrComputeLineages(
      outputDirectory,
      mutations.select($"mutationId", $"parentMutationId").as[MutationTreeLink]
    )

    spark.sparkContext.setJobGroup("muller order", "collect muller order for large mutations")
    val largeMullerOrder = Muller.readOrComputeLargeMullerOrder(largeMullerOrderPath,lineages, largeClones)
    /*
    val largeMullerOrder: Array[MutationOrder] = Muller.mullerOrder(
      lineages.
        join(broadcast(largeClones.select("mutationId")),"mutationId").
        as[Ancestry]
      ).
      orderBy("ordering").
      collect()
    */
/*
    spark.sparkContext.setJobGroup("large clones", "save large clones")
    saveParquet(largeClonesPath,
      largeClones.join(broadcast(largeMullerOrder.toSeq.toDS()), "mutationId").
        orderBy("ordering").
        select(
          col("mutationId"),
          col("cellParams.birthEfficiency"),
          col("cellParams.birthResistance"),
          col("cellParams.lifespanEfficiency"),
          col("cellParams.lifespanResistance"),
          col("cellParams.successEfficiency"),
          col("cellParams.successResistance")
        )
    )*/

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
    saveParquet(largeFinalMutationsPath, largeFinalMutations.filter(col("mutationCount")>=1000).toDF())

    finalClones.unpersist()

  }
}
