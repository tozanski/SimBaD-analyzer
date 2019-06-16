package analyzer

import org.apache.spark.sql.{Column, Dataset, Encoders, Row, SparkSession, TypedColumn}
import org.apache.spark.sql.functions.{array, avg, col, concat, count, explode, hypot, lit, max, stddev, struct, when}

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

  def OnePassStats(chronicles: Dataset[ChronicleEntry], timePoints: Seq[Double]): Array[CellStats] = {

    def colOrNull(value: Column, timePoint: Double): Column = {
      when(
        col("birthTime") < lit(timePoint) && lit(timePoint) < col("deathTime"),
        value
      ).otherwise(null)
    }

    def timePointStats(timePoint: Double): TypedColumn[Any, CellStats] = {
      struct(
        count(colOrNull(lit(1), timePoint)).alias("count"),
        max(colOrNull(hypot(hypot("position.x", "position.y"), "position.z"), timePoint)).alias("max_origin_distance"),

        avg(colOrNull(col("mutation.birthEfficiency"), timePoint)).alias("mean_birth_efficiency"),
        avg(colOrNull(col("mutation.birthResistance"), timePoint)).alias("mean_birth_resistance"),
        avg(colOrNull(col("mutation.lifespanEfficiency"), timePoint)).alias("mean_lifespan_efficiency"),
        avg(colOrNull(col("mutation.lifespanResistance"), timePoint)).alias("mean_lifespan_resistance"),
        avg(colOrNull(col("mutation.successEfficiency"), timePoint)).alias("mean_success_efficiency"),
        avg(colOrNull(col("mutation.successResistance"), timePoint)).alias("mean_success_resistance"),

        stddev(colOrNull(col("mutation.birthEfficiency"), timePoint)).alias("stddev_birth_efficiency"),
        stddev(colOrNull(col("mutation.birthResistance"), timePoint)).alias("stddev_birth_resistance"),
        stddev(colOrNull(col("mutation.lifespanEfficiency"), timePoint)).alias("stddev_lifespan_efficiency"),
        stddev(colOrNull(col("mutation.lifespanResistance"), timePoint)).alias("stddev_lifespan_resistance"),
        stddev(colOrNull(col("mutation.successEfficiency"), timePoint)).alias("stddev_success_efficiency"),
        stddev(colOrNull(col("mutation.successResistance"), timePoint)).alias("stddev_success_resistance")
      ).alias("cell_stats").as(Encoders.product[CellStats])
    }

    val aggregates = timePoints.
      zipWithIndex.
      map({case(t,idx) => timePointStats(t).alias("cell_stats_"+idx.toString)})

    chronicles.sparkSession.sparkContext.setJobGroup("cell stats", "single pass cell stats")
    chronicles.
      agg(
        aggregates.head, aggregates.tail:_*
      ).
      select(
        array(
          timePoints.zipWithIndex.map({case(_,idx) => col("cell_stats_"+idx.toString)}):_*
        ).alias("aggregated")
      ).
      select(explode(col("aggregated"))).
      select("col.*").
      as(Encoders.product[CellStats]).
      collect()
  }

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
    cells.sparkSession.sparkContext.setJobGroup("cell stats", " compute cell stats in one pass")
    compute(cells).collect()(0)
  }
}
