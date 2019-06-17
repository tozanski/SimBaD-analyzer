package analyzer

import java.io.{File, PrintWriter}

import analyzer.CloneStats.ScalarCloneStats
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, Row}
import org.apache.spark.sql.functions.{col, count, explode, first, lit, log2, struct, sum}
import org.apache.spark.sql.types.{ArrayType, DataType, FloatType, LongType, StructField, StructType}

import scala.math.{floor, max, min}

case class CloneStats(
  timePoint: Double,
  scalarStats: ScalarCloneStats,
  histograms: CloneStats.CellHistogram
)

object CloneStats {
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
    entropy: Double
  )

  def computeMutationFrequency(clones: Dataset[Clone], lineages: Dataset[Ancestry]): Dataset[(Long, Long)] = {
    clones.
      select("mutationId", "count").join(lineages, "mutationId").
      withColumn("ancestorMutationId", explode(col("ancestors"))).
      groupBy("ancestorMutationId").
      agg(
        sum(col("count"))
      ).
      as(Encoders.product[(Long,Long)])
  }

  def scalarAggregate(): Column = {
    struct(
      count(lit(1)).alias("cloneCount"),
      first("systemSize").alias("systemSize"),
      sum(col("probability") * -log2("probability") ).alias("entropy")
    ).alias("scalarStats")
  }

  def compute(cloneSnapshots: Dataset[CloneSnapshot]): Dataset[CloneStats] = {
    val systemSizes = cloneSnapshots.
      groupBy("timePoint").
      agg(
        sum(col("count")).alias("systemSize")
      ).
      select(
        col("timePoint").as(Encoders.scalaDouble),
        col("systemSize").as(Encoders.scalaLong)
      )

    val aggregates = histogramAggregate() :: scalarAggregate() :: Nil

    cloneSnapshots.
      join(systemSizes, "timePoint").
      withColumn("probability",
        col("count")/col("systemSize")
      ).
      groupBy("timePoint").
      agg(aggregates.head, aggregates.tail:_*).
      as(Encoders.product[CloneStats])
  }

  def collect(cloneSnapshots: Dataset[CloneSnapshot]): Array[CloneStats] = {
    cloneSnapshots.sparkSession.sparkContext.setJobGroup("clone stats", "compute clone stats")
    compute(cloneSnapshots).
      coalesce(1).
      sortWithinPartitions("timePoint").
      collect()
  }

  def histogramAggregate(): Column = {
    val hist = new HistogramUDAF
    val aggregates = mutationParameterNames.
      map(x => hist.apply(col("mutation."+ x), col("count")).alias(x))
    struct(aggregates:_*).alias("histograms")
  }

  class HistogramUDAF extends UserDefinedAggregateFunction {

    override def inputSchema:StructType = StructType(
      StructField("value", FloatType)::
      StructField("weight", LongType)::
      Nil
    )

    override def bufferSchema: StructType = StructType(StructField("counts", ArrayType(LongType))::Nil)

    override def dataType: DataType = ArrayType(LongType)

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = Array.fill[Long](100)(0l)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if(input.isNullAt(0))
        return

      val num_bins = 100

      val param_value: Float = input.getAs[Float](0)
      val param_count: Long = input.getAs[Long](1)

      val bin_number: Int = math.min(num_bins-1, math.max(0, math.floor(param_value * num_bins).toInt))

      val bins: Array[Long]  = buffer.getSeq[Long](0).toArray[Long]
      bins(bin_number) += param_count
      buffer(0) = bins
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val arr1 = buffer1.getSeq[Long](0)
      val arr2 = buffer2.getSeq[Long](0)

      val result = (arr1, arr2).zipped.map(_ + _)
      buffer1(0) = result
    }

    override def evaluate(buffer: Row): Any = buffer.getAs[Array[Long]](0)

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
  def writeScalars(filePath: String, stats: Seq[ScalarCloneStats] ) = {
    val pw = new PrintWriter(new File(filePath))
    stats.foreach(s=> pw.println(s.cloneCount + ";" + s.systemSize + ";" + s.entropy))
  }

}
