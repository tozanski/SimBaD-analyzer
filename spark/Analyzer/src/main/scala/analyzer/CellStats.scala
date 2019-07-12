package analyzer

import java.io.{File, PrintWriter}

import analyzer.CellStats.ScalarCloneStats
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, Row, SaveMode}
import org.apache.spark.sql.functions.{avg, broadcast, col, count, explode, first, lit, log2, struct, sum, udf}
import org.apache.spark.sql.types.{ArrayType, DataType, FloatType, LongType, StructField, StructType}

import scala.math.{floor, max, min}
import analyzer.expression.functions.{weightedAvg, weightedStdDev}

case class CellStats(
  timePoint: Double,
  scalarStats: ScalarCloneStats,
  histograms: CellStats.CellHistogram
)

object CellStats {
  val mutationParameterNames: Seq[String] =
    "birthEfficiency" :: "birthResistance" ::
    "lifespanEfficiency" :: "lifespanResistance" ::
    "successEfficiency" :: "successResistance" ::
    Nil

  case class CellHistogram(
    birthEfficiency: Array[Long],
    birthResistance: Array[Long],
    lifespanEfficiency: Array[Long],
    lifespanResistance: Array[Long],
    successEfficiency: Array[Long],
    successResistance: Array[Long]
  )

  case class ScalarCloneStats(
    cloneCount: Long,
    systemSize: Long,
    entropy: Double,
    normalizedEntropy: Double,

    mean_birth_efficiency: Double, mean_birth_resistance: Double,
    mean_lifespan_efficiency: Double, mean_lifespan_resistance: Double,
    mean_success_efficiency: Double, mean_success_resistance: Double,

    stddev_birth_efficiency: Double, stddev_birth_resistance: Double,
    stddev_lifespan_efficiency: Double, stddev_lifespan_resistance: Double,
    stddev_success_efficiency: Double, stddev_success_resistance: Double
  )

  def computeMutationFrequency(clones: Dataset[Clone], lineages: Dataset[Ancestry]): Dataset[(Long, Long)] = {
    clones.
      select("mutationId", "count").join(lineages, "mutationId").
      withColumn("ancestorMutationId", explode(col("ancestors"))).
      groupBy("ancestorMutationId").
      agg(
        sum(col("count")).alias("mutationCount")
      ).
      as(Encoders.product[(Long,Long)])
  }

  def scalarAggregate(): Column = {
    struct(
      count(lit(1)).alias("cloneCount"),
      first("systemSize").alias("systemSize"),
      sum(col("probability") * -log2("probability") ).alias("entropy"),
      (sum(col("probability") * -log2("probability")) / log2(count(lit(1)))).alias("normalizedEntropy"),

      weightedAvg(col("cellParams.birthEfficiency"), col("count")).alias("mean_birth_efficiency"),
      weightedAvg(col("cellParams.birthResistance"), col("count")).alias("mean_birth_resistance"),
      weightedAvg(col("cellParams.lifespanEfficiency"), col("count")).alias("mean_lifespan_efficiency"),
      weightedAvg(col("cellParams.lifespanResistance"), col("count")).alias("mean_lifespan_resistance"),
      weightedAvg(col("cellParams.successEfficiency"), col("count")).alias("mean_success_efficiency"),
      weightedAvg(col("cellParams.successResistance"), col("count")).alias("mean_success_resistance"),

      weightedStdDev(col("cellParams.birthEfficiency"), col("count")).alias("stddev_birth_efficiency"),
      weightedStdDev(col("cellParams.birthResistance"), col("count")).alias("stddev_birth_resistance"),
      weightedStdDev(col("cellParams.lifespanEfficiency"), col("count")).alias("stddev_lifespan_efficiency"),
      weightedStdDev(col("cellParams.lifespanResistance"), col("count")).alias("stddev_lifespan_resistance"),
      weightedStdDev(col("cellParams.successEfficiency"), col("count")).alias("stddev_success_efficiency"),
      weightedStdDev(col("cellParams.successResistance"), col("count")).alias("stddev_success_resistance")
    ).alias("scalarStats")
  }

  def write(path: String, cloneSnapshots: Dataset[CloneSnapshot]): Array[CellStats] = {


    cloneSnapshots.sparkSession.sparkContext.setJobGroup("system sizes", "collect system sizes")
    val systemSizes: Map[Double, Long] = cloneSnapshots.
      groupBy("timePoint").
      agg(
        sum(col("count")).alias("systemSize")
      ).
      select(
        col("timePoint").as(Encoders.scalaDouble),
        col("systemSize").as(Encoders.scalaLong)
      ).collect().toMap

    val systemSizesBc = cloneSnapshots.sparkSession.sparkContext.broadcast(systemSizes)
    val mapSystemSizeUDF = udf( (x:Double) => systemSizesBc.value.get(x))


    val aggregates = histogramAggregate() :: scalarAggregate() :: Nil

    cloneSnapshots.sparkSession.sparkContext.setJobGroup("clone stats", "compute clone stats")
    cloneSnapshots.
      withColumn("systemSize", mapSystemSizeUDF(col("timePoint"))).
      withColumn("probability",
        col("count")/col("systemSize")
      ).
      groupBy("timePoint").
      agg(aggregates.head, aggregates.tail:_*).
      as(Encoders.product[CellStats]).
       orderBy("timePoint").
      write.
      mode(SaveMode.Overwrite).
      parquet(path)

    systemSizesBc.unpersist()
    cloneSnapshots.sparkSession.read.parquet(path).as(Encoders.product[CellStats]).collect()
  }

  def readOrCompute(path: String, cloneSnapshots: Dataset[CloneSnapshot]): Array[CellStats] = {
    val spark = cloneSnapshots.sparkSession
    import spark.implicits._

    try{
      spark.read.parquet(path).as[CellStats].orderBy("timePoint").collect()
    }catch{
      case _: Exception =>
        write(path, cloneSnapshots)
        spark.read.parquet(path).as[CellStats].orderBy("timePoint").collect()
    }
  }


  def histogramAggregate(): Column = {
    val hist = new expression.HistogramUDAF
    val aggregates = mutationParameterNames.
      map(x => hist.apply(col("cellParams."+ x), col("count")).alias(x))
    struct(aggregates:_*).alias("histograms")
  }

  def writeHistograms(pathPrefix: String, histograms: Dataset[CellHistogram]) = {

    for (parameterName: String <- mutationParameterNames) {
      histograms.
        select(col(parameterName).as(ExpressionEncoder[Array[Long]])).
        rdd.
        map( _.mkString(":")).
        saveAsTextFile(pathPrefix+"histogram_"+parameterName)
    }
  }
  def writeHistograms(pathPrefix: String, histograms: Seq[CellHistogram]) = {
    for (parameterName: String <- mutationParameterNames) {
      val filePath = pathPrefix + "histogram_"+parameterName+".csv"
      val pw = new PrintWriter(new File(filePath))
      for (histogramPack: CellHistogram <- histograms) {
        val histogramField = histogramPack.
          getClass.
          getDeclaredField(parameterName)
        histogramField.setAccessible(true)

        val histogram: Array[Long] = histogramField.
          get(histogramPack).
          asInstanceOf[Array[Long]]

        pw.println(histogram.mkString(";"))
      }
      pw.close()
    }
  }

  def writeScalars(filePath: String, stats: Seq[ScalarCloneStats]) = {
    val pw = new PrintWriter(new File(filePath))
    stats.foreach(stat => {
      stat.getClass.getDeclaredFields.foreach(
        field => {
          field.setAccessible(true)
          pw.print(field.get(stat))
        }
      )
      pw.println()
    })
    pw.close()
  }
}
