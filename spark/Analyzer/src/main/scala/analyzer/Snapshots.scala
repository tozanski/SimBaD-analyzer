package analyzer

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Encoders, SaveMode, SparkSession}

object Snapshots{
  def snapshotsUdf(maxTime: Double) = udf(
      (t1:Double, t2:Double) => (0d to maxTime by 1).filter( t => t1 <= t && t < t2 )
    )
  def snapshotsUdf(timePoints: Seq[Double]) = udf(
    (t1: Double, t2:Double) => timePoints.filter(t => t1<=t && t<t2)
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
        col("cellParams").as(Encoders.product[CellParams])).
      as(Encoders.product[Cell])
  }


  def getCloneSnapshot(cells: Dataset[Cell]): Dataset[Clone] = {
    cells.
      groupBy("mutationId").
      agg(
        count(lit(1)).as("count"),
        first(col("cellParams")).as("cellParams")
      ).
      as(Encoders.product[Clone])
  }

  def getCloneSnapshots(chronicles: Dataset[ChronicleEntry], maxTime: Double): DataFrame = {
    chronicles.
      repartition(col("mutationId")).
      withColumn("timePoint",
        explode(
          sequence(
            greatest(lit(0), col("birthTime")).cast(IntegerType),
            least(lit(maxTime), col("deathTime")).cast(IntegerType)
          )
        )
      ).
      groupBy("mutationId","timePoint").
      agg(
        count(lit(1)).as("count"),
        first(col("cellParams")).as("cellParams")
      )
  }

  def getCloneSnapshots(chronicles: Dataset[ChronicleEntry], timePoints: Seq[Double]) : DataFrame = {
    chronicles.
      repartition(col("mutationId")).
      withColumn("timePoint",
      explode(
        snapshotsUdf(timePoints)(col("birthTime"), col("deathTime")))
      ).groupBy("mutationId", "timePoint").
      agg(
        count(lit(1)).alias("count"),
        first(col("cellParams")).alias("cellParams")
      )
  }

  def computeOrReadCloneSnapshots(pathPrefix: String,
                                  chronicles: Dataset[ChronicleEntry],
                                  timePoints: Seq[Double],
                                  partitionByTime: Boolean = false): Dataset[CloneSnapshot] ={
    val spark = chronicles.sparkSession
    val path = pathPrefix + "/clone_snapshots.parquet"
    try{
      spark.sparkContext.setJobGroup("read clone snapshots","read clone snapshots")
      spark.read.parquet(path).as(Encoders.product[CloneSnapshot])
    } catch {
      case _: AnalysisException =>
        spark.sparkContext.setJobGroup("clone snapshots","save clone snapshots")
        getCloneSnapshots(chronicles, timePoints).
          write.
          partitionBy("timePoint").
          mode(SaveMode.Overwrite).
          parquet(path)

        spark.read.parquet(path).as(Encoders.product[CloneSnapshot])
    }
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
          first("cellParams.birthEfficiency").alias("birth_efficiency"),
          first("cellParams.birthResistance").alias("birth_resistance"),
          first("cellParams.lifespanEfficiency").alias("lifespan_efficiency"),
          first("cellParams.lifespanResistance").alias("lifespan_resistance"),
          first("cellParams.successEfficiency").alias("success_efficiency"),
          first("cellParams.successResistance").alias("success_resistance")
        )
  }

  def finalCloneSnapshot(chronicles: Dataset[ChronicleEntry]): Dataset[Clone] = chronicles.
    filter(col("deathTime")===Double.PositiveInfinity).
    groupBy("mutationId").
    agg(
      count(lit(1)).as("count"),
      first(col("cellParams")).as("cellParams")
    ).
    as(Encoders.product[Clone])

  def getFinalCells(chronicleEntries: Dataset[ChronicleEntry]): DataFrame = chronicleEntries.
    filter( col("deathTime") === Double.PositiveInfinity ).
      select(
        "position.x", "position.y", "position.z",
        "mutationId",
        "cellParams.birthEfficiency", "cellParams.birthResistance",
        "cellParams.lifespanEfficiency", "cellParams.lifespanResistance",
        "cellParams.successEfficiency", "cellParams.successResistance"
      )


/*
  def getFinalSimpleMutationHistogram( finalConfiguration: DataFrame ): DataFrame = {
    finalConfiguration.groupBy("mutationId").count().orderBy("cellParamsId")
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
