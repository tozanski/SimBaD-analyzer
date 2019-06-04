package analyzer

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object Snapshots{
  def snapshotsUdf(maxTime: Double) = udf(
      (t1:Double, t2:Double) => (0d to maxTime by 1).filter( t => t1 <= t && t < t2 )
    )

  def getSnapshots( chronicles: Dataset[ChronicleEntry], maxTime: Double ): DataFrame = {
    chronicles.withColumn("timePoint", explode(snapshotsUdf(maxTime)(col("birthTime"), col("deathTime"))))
  }

  def getSnapshot(chronicles: Dataset[ChronicleEntry], timePoint: Double): Dataset[Cell] = {
    chronicles.
      filter(col("birthTime") <= lit(timePoint) && lit(timePoint) < col("deathTime") ).
      select(
        col("position").as(Encoders.product[Position]),
        col("mutationId").as(Encoders.LONG),
        col("mutation").as(Encoders.product[Mutation])).
      as(Encoders.product[Cell])
  }

  def getSnapshotList(chronicles: Dataset[ChronicleEntry], timePoints: Iterable[Double]): Vector[Dataset[ChronicleEntry]] = {
    timePoints.map(t => chronicles.filter(
      col("birthTime") < lit(t) && lit(t) < col("deathTime"))
    ).toVector
  }

  def mutationSnapshots( snapshots: DataFrame ): DataFrame = {
      snapshots.
        groupBy("timePoint","mutationId").
        agg(
          count(lit(1)),
          first("mutation.birthEfficiency").alias("birth_efficiency"),
          first("mutation.birthResistance").alias("birth_resistance"),
          first("mutation.lifespanEfficiency").alias("lifespan_efficiency"),
          first("mutation.lifespanResistance").alias("lifespan_resistance"),
          first("mutation.successEfficiency").alias("success_efficiency"),
          first("mutation.successResistance").alias("success_resistance")
        )
  }

  def getFinal( chronicleEntries: Dataset[ChronicleEntry]): DataFrame = {
    // final
    chronicleEntries.filter( col("deathTime") === Double.PositiveInfinity ).
      select(
        "position.x", "position.y", "position.z",
        "mutationId",
        "mutation.birthEfficiency", "mutation.birthResistance",
        "mutation.lifespanEfficiency", "mutation.lifespanResistance",
        "mutation.successEfficiency", "mutation.successResistance"
      )
  }
/*
  def getFinalSimpleMutationHistogram( finalConfiguration: DataFrame ): DataFrame = {
    finalConfiguration.groupBy("mutationId").count().orderBy("mutationId")
  }*/

  def writeSnapshots( chronicles: Dataset[ChronicleEntry], pathPrefix: String, maxTime: Double ) = {
    // snapshots
    for( t <- 0 to maxTime.toInt by 1 ){
      chronicles.
      filter( (col("birth_time") < lit(t) ) && (col("death_time") > lit(t) )  ).
      coalesce(1).
      write.
      format("csv").
      option("delimiter",";").
      option("header", "true").
      mode("overwrite").
      save(pathPrefix + "/snapshots/" + "%05d".format(t) )
    }
  }
}
