package analyzer

import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.sql.functions.{count, lit}

case class CloneStats (
  count: Long,
  entropy: Double

)

object CloneStats {
  def compute( dataset: Dataset[Clone]): Dataset[CloneStats] = {
    dataset.agg(
      count(lit(1)).as("count")
    ).as(Encoders.product[CloneStats])
  }
  def collect(dataset: Dataset[Clone]): CloneStats = {
    compute(dataset).collect()(0)
  }
}
