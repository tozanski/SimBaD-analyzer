package analyzer

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.{avg, count, hypot, lit, max, stddev}

case class CellStats(
  count: Long, max_origin_distance: Double,
  mean_birth_efficiency: Double, mean_birth_resistance: Double,
  mean_lifespan_efficiency: Double, mean_lifespan_resistance: Double,
  mean_success_efficiency: Double, mean_success_resistance: Double,

  stddev_birth_efficiency: Double, stddev_birth_resistance: Double,
  stddev_lifespan_efficiency: Double, stddev_lifespan_resistance: Double,
  stddev_success_efficiency: Double, stddev_success_resistance: Double
)

object CellStats {
  def compute(cells: Dataset[Cell]): Dataset[CellStats] = {

    cells.
      agg(
        count(lit(1)).alias("count"),
        max(hypot(hypot("position.x", "position.y"), "position.z")).alias("max_origin_distance"),

        // means
        avg("mutation.birthEfficiency").alias("mean_birth_efficiency"),
        avg("mutation.birthResistance").alias("mean_birth_resistance"),
        avg("mutation.lifespanEfficiency").alias("mean_lifespan_efficiency"),
        avg("mutation.lifespanResistance").alias("mean_lifespan_resistance"),
        avg("mutation.successEfficiency").alias("mean_success_efficiency"),
        avg("mutation.successResistance").alias("mean_success_resistance"),

        // stdev
        stddev("mutation.birthEfficiency").alias("stddev_birth_efficiency"),
        stddev("mutation.birthResistance").alias("stddev_birth_resistance"),
        stddev("mutation.lifespanEfficiency").alias("stddev_lifespan_efficiency"),
        stddev("mutation.lifespanResistance").alias("stddev_lifespan_resistance"),
        stddev("mutation.successEfficiency").alias("stddev_success_efficiency"),
        stddev("mutation.successResistance").alias("stddev_success_resistance")

      ).as(Encoders.product[CellStats])
  }

  def collect(cells: Dataset[Cell]): CellStats = {
    compute(cells).collect()(0)
  }
}
