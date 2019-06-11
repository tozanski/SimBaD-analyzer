package analyzer

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}
import org.apache.spark.sql.functions.{col, count, explode, first, lit, log2, sum}
import org.apache.spark.sql.types.{FloatType, LongType, StructField, StructType}

case class CloneStats(
  cloneCount: Long,
  totalCount: Long,
  cloneEntropy: Double
)

object CloneStats {
  case class SimpleStats(
    cloneCount: Long,
    totalCount: Long
  )

  def collectSimpleStats(clones: Dataset[Clone]): SimpleStats = {
    clones.
      agg(
        count(lit(1)).as("cloneCount"),
        sum(col("count")).as("totalCount")
      ).
      as(Encoders.product[SimpleStats]).
      collect()(0)
  }


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

  def computeEntropy(clones: Dataset[Clone], totalCount: Long): Double = {
    clones.
      withColumn("probability", col("count")/totalCount).
      agg(
        (-sum(col("probability") * log2(col("probability")))).as("entropy")
      ).
      select(col("entropy").as(Encoders.scalaDouble)).
      as(Encoders.scalaDouble).
      collect()(0)
  }

  def collect(clones: Dataset[Clone]): CloneStats = {

    val simpleStats = collectSimpleStats(clones)
    val entropy = computeEntropy(clones, simpleStats.totalCount)

    CloneStats(
      simpleStats.cloneCount,
      simpleStats.totalCount,
      entropy
    )
  }


}
