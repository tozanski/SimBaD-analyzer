package analyzer

import java.io.{File, PrintWriter}

import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, Row}
import org.apache.spark.sql.functions.{array_join, col, collect_list, lit}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.DoubleRDDFunctions
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, FloatType, LongType, StructField, StructType}

import scala.math.{floor, max, min}

case class CellHistogram(
  birthEfficiency: Array[Long],
  birthResistance: Array[Long],
  lifespanEfficiency: Array[Long],
  lifespanResistance: Array[Long],
  successEfficiency: Array[Long],
  successResistance: Array[Long]
)

object CellHistogram {
  val mutationParameterNames: Seq[String] =
    "birthEfficiency" :: "birthResistance" ::
      "lifespanEfficiency" :: "lifespanResistance" ::
      "successEfficiency" :: "successResistance" :: Nil

  class HistogramUDAF extends UserDefinedAggregateFunction {

    override def inputSchema:StructType = StructType(StructField("value", FloatType)::Nil)

    override def bufferSchema: StructType = StructType(StructField("counts", ArrayType(LongType))::Nil)

    override def dataType: DataType = ArrayType(LongType)

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = Array.fill[Long](100)(0l)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val num_bins = 100

      val param_value: Float = input.getAs[Float](0)
      val bin_number: Int = min(num_bins-1, max(0, floor(param_value * num_bins).toInt))

      val bins: Array[Long]  = buffer.getSeq[Long](0).toArray[Long]
      bins(bin_number) += 1
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

  def compute(cells: Dataset[Cell]): Dataset[CellHistogram] = {
    val hist = new HistogramUDAF
    val aggregates: Seq[Column] = mutationParameterNames.map( x => hist.apply(col("mutation."+ x)).alias(x))

    cells.agg(
      aggregates(0),
      aggregates(1),
      aggregates(2),
      aggregates(3),
      aggregates(4),
      aggregates(5)
    ).as(Encoders.product[CellHistogram])
  }

  def collect(cells: Dataset[Cell]): CellHistogram = {
    cells.sparkSession.sparkContext.setJobGroup("histogram", "save cell histogram")
    compute(cells).collect()(0)
  }

  def writeDF(pathPrefix: String, histograms: Dataset[CellHistogram]) = {

    for (parameterName: String <- mutationParameterNames) {
          histograms.
            select(col(parameterName).as(ExpressionEncoder[Array[Long]])).
            rdd.
            map( _.mkString(":")).
            saveAsTextFile(pathPrefix+"histogram_"+parameterName)
    }
  }
  def writeArr(pathPrefix: String, histograms: Array[CellHistogram]) = {
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

}
