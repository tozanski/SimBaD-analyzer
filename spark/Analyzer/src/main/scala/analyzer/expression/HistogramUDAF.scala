package analyzer.expression

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, FloatType, LongType, StructField, StructType}

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
