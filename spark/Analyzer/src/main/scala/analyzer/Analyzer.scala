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

  def readTimePoints(spark: SparkSession, path: String ): Seq[Double] ={
    import spark.implicits._

    val schema = StructType(Array(
      StructField("timePoint", FloatType, nullable=false)
    ))

    spark.
      read.
      format("csv").
      option("positiveInf", "inf").
      option("negativeInf", "-inf").
      option("header", "false").
      option("delimiter", ";").
      option("mode", "DROPMALFORMED").
      schema(schema).
      load(path).
      as[Double].
      collect().
      toArray
  }

  def getOrReadTimePoints(chronicles: Dataset[ChronicleEntry], outputPath: String ): Seq[Double] = {
    val spark = chronicles.sparkSession
    val path = outputPath + "/timePoints.csv"
    try{
      readTimePoints(spark, path)
    }catch{
      case _: Exception =>
        val maxTime =  getMaxTime(chronicles)
        val timePoints = (0d until maxTime by 1.0d) :+ maxTime
        saveCSV(path, timePoints)
        timePoints
    }
  }

  def saveParquet(path: String, dataFrame: DataFrame, coalesce: Boolean): Unit = {
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

    val timePointsPath = outputDirectory + "timePoints.csv"
    val largeMutationsPath = outputDirectory + "large_mutations.csv"
    val finalSnapshotPath = outputDirectory + "final_snapshot.csv"
    val cloneStatsPath = outputDirectory + "clone_stats.csv"
    val mullerPlotDataPath = outputDirectory + "muller_data.csv"
    val finalMutationFrequencyPath = outputDirectory + "final_mutation_freq.csv"
    val cloneCountsPath = outputDirectory + "clone_counts.parquet"

    val chronicles = Chronicler.
      computeOrReadChronicles(spark, streamPath, outputDirectory)
      //.coalesce(2). // debug only
      //persist() // debug only

    val maxTime =  getMaxTime(chronicles)
    //val maxTime = 20.0 // debug only

    val timePoints = (0d until maxTime by 1.0d) :+ maxTime
    saveCSV(timePointsPath, timePoints)

    //val timePoints = getOrReadTimePoints(chronicles, outputDirectory)

    val cloneSnapshots = Snapshots.computeOrReadCloneSnapshots(outputDirectory, chronicles, timePoints, partitionByTime = false)

    val cloneStats = CellStats.collect(cloneSnapshots)
    CellStats.writeHistograms(outputDirectory, cloneStats.map(_.histograms))
    saveCSV(cloneStatsPath, cloneStats.map(_.scalarStats).toSeq.toDS().toDF(), coalesce = true)

    val largeMutations: Dataset[(Long, CellParams)] = chronicles.
      groupBy("mutationId").
      agg(
        count(lit(1)).alias("mutationSize"),
        first(col("cellParams")).alias("cellParams")
      ).
      filter( $"mutationSize" > 1000 ).
      select($"mutationId".as[Long], $"cellParams".as[CellParams]).
      cache()
    spark.sparkContext.setJobGroup("large mutations", "count large mutations")
    println("Large mutation count" + largeMutations.count())

    val mutations = Phylogeny.getOrComputeMutationBranches(outputDirectory, chronicles)
    val lineages = Phylogeny.getOrComputeLineages(
      outputDirectory,
      mutations.select($"mutationId", $"parentMutationId").as[MutationTreeLink]
    )

    spark.sparkContext.setJobGroup("muller order", "collect muller order for large mutations")
    val largeMullerOrder: Array[MutationOrder] = Muller.mullerOrder(
      lineages.
        join(broadcast(largeMutations.select("mutationId")),"mutationId").
        as[Ancestry]
      ).
      orderBy("ordering").
      collect()


    spark.sparkContext.setJobGroup("large mutations", "save large mutations")
    saveCSV(largeMutationsPath,
      largeMutations.join(broadcast(largeMullerOrder.toSeq.toDS()), "mutationId").
        orderBy("ordering").
        select(
          col("mutationId"),
          col("cellParams.birthEfficiency"),
          col("cellParams.birthResistance"),
          col("cellParams.lifespanEfficiency"),
          col("cellParams.lifespanResistance"),
          col("cellParams.successEfficiency"),
          col("cellParams.successResistance")
        ),
      coalesce=true)

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
    saveCSV(
      finalMutationFrequencyPath,
      CellStats.computeMutationFrequency(
        finalClones,
        lineages
      ).orderBy("ancestorMutationId").toDF(),
      coalesce = true)

    spark.sparkContext.setJobGroup("mutation counts", "save mutation counts")
    saveParquet(cloneCountsPath, Phylogeny.mutationCounts(lineages, finalClones, mutations).toDF(), coalesce=false)
    finalClones.unpersist()

  }
}
