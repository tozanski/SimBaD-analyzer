package analyzer

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, DataFrame, Encoders, SaveMode, SparkSession}

object Muller{
  def mullerOrder(lineages: Dataset[Ancestry] ): Dataset[MutationOrder] = {

    import lineages.sparkSession.implicits._

    implicit val mutationOrder = new Ordering[Iterable[Long]]{
      override def compare( lhs: Iterable[Long], rhs: Iterable[Long]): Int = {
        var itL = lhs.iterator
        val itR = rhs.iterator

        while( true )
        {
          if( !itL.hasNext && !itR.hasNext) return 0;// are same
          if( itL.hasNext && !itR.hasNext)  return +1// rhs is shorter
          if( !itL.hasNext && itR.hasNext) return -1;// lhs is shorter

          val valL = itL.next
          val valR = itR.next

          if( valL < valR ) return +1
          if( valL > valR ) return -1
        }
        return 0
      }
    }

    lineages.
      rdd.
      map( x => (x.mutationId, x.ancestors.toIterable) ).
      sortBy( _._2 ).
      //map( x => (x._1, x._2.toArray) ).
      //toDF("cellParamsId","ancestors").
      map( _._1).
      toDF("mutationId").
      withColumn("ordering", monotonically_increasing_id).
      as[MutationOrder]
  }

  def largeMullerOrder(lineages: Dataset[Ancestry], largeClones: Dataset[(Long,CellParams)]): Array[MutationOrder] ={
    Muller.mullerOrder(
      lineages.
        join(broadcast(largeClones.select("mutationId")),"mutationId").
        as(Encoders.product[Ancestry])
    ).
      orderBy("ordering").
      collect()
  }

  def readOrComputeLargeMullerOrder(path: String,
                                    lineages: Dataset[Ancestry],
                                    largeClones: Dataset[(Long, CellParams)]): Array[MutationOrder] = {
    val spark = lineages.sparkSession
    import spark.implicits._

    try{
      spark.read.parquet(path).as(Encoders.product[MutationOrder]).collect()
    }catch{
      case _: Exception =>
        spark.sparkContext.setJobGroup("large muller order", "large muller order")
        val result = largeMullerOrder(lineages, largeClones)
        result.toSeq.toDS().toDF().write.mode(SaveMode.Overwrite).parquet(path)
        result
    }
  }

  def largeClones(chronicles: Dataset[ChronicleEntry], threshold: Long): Dataset[(Long, CellParams)] = {
    chronicles.
      groupBy("mutationId").
      agg(
        count(lit(1)).alias("mutationSize"),
        first(col("cellParams")).alias("cellParams")
      ).
      filter( col("mutationSize") > threshold ).
      select(
        col("mutationId").as(Encoders.scalaLong),
        col("cellParams").as(Encoders.product[CellParams])
      )
  }
  def readOrComputeLargeClones(path: String, chronicles: Dataset[ChronicleEntry], threshold: Long): Dataset[(Long, CellParams)] = {
    val spark = chronicles.sparkSession
    import spark.implicits._
    try{
      spark.read.parquet(path).as[(Long, CellParams)]
    }catch {
      case _: Exception =>
        spark.sparkContext.setJobGroup("large clones", "compute large clones")
        largeClones(chronicles, threshold).write.mode(SaveMode.Overwrite).parquet(path)
        spark.read.parquet(path).as[(Long, CellParams)]
    }
  }


  def mullerData( spark: SparkSession,
                  chronicles: Dataset[ChronicleEntry],
                  lineages: Dataset[Ancestry],
                  maxTime: Double,
                  minCellCount: Long ): Dataset[(Long, Double, Double)] = {
    import spark.implicits._

    val mutationSizes: Dataset[(Long,Long)] = chronicles.
      groupBy("mutationId").
      agg( count(lit(1)).alias("mutationSize") ).
      as[(Long,Long)]

    val snapshots: Dataset[(Long, Double)] = chronicles.
      join(mutationSizes, "mutationId").
      withColumn("aggMutationId", when($"mutationSize" < 1000, 0l).otherwise($"mutationId")).
      withColumn("timePoint", explode(Snapshots.snapshotsUdf(maxTime)(col("birthTime"), col("deathTime")))).
      select(
        $"aggMutationId".alias("mutationId").as[Long],
        $"timePoint".as[Double])

    val orderedMutations: Dataset[MutationOrder] = mullerOrder(lineages)

    val mullerCumulatives = snapshots.
      join(orderedMutations,Seq("mutationId"), "left").
      withColumn("cumeDist",
        cume_dist over Window.partitionBy("timePoint").orderBy("ordering")).
      dropDuplicates.
      orderBy("ordering","timePoint").
      drop("ordering").
      as[(Long, Double, Double)]

    mullerCumulatives
  }


  def writePlotData(path: String, cloneSnapshots: Dataset[CloneSnapshot], largeMutations: Seq[Long] ): Unit ={
    val spark = cloneSnapshots.sparkSession
    import spark.implicits._

    val mutationSet = spark.sparkContext.broadcast(largeMutations.toSet)
    val collapseMutationId: Long => Long = x => if(mutationSet.value.contains(x)) x else 0
    val collapseMutationsUDF = udf(collapseMutationId)

    spark.sparkContext.setJobGroup("muller snapshot", "muller plot data snapshot")
    val result = cloneSnapshots.
      withColumn("collapsedMutationId", collapseMutationsUDF($"mutationId")).
      groupBy("timePoint").
      pivot("collapsedMutationId", 0+:largeMutations).
      agg(
        //first(when(isnull(col("ordering")), lit(0)).otherwise(col("mutationId"))).as("mutationId"),

        sum(coalesce(col("count"), lit(0))).as("count")
      ).
      na.fill(0).
      orderBy("timePoint")

    spark.sparkContext.setJobGroup("muller plot data", "write muller plot data")
    Analyzer.saveParquet(path, result)

    mutationSet.destroy()

  }

  def collect(clones: Dataset[Clone], mutationOrder: Dataset[MutationOrder]): Array[Long] = {
    import clones.sparkSession.implicits._

    val defaultClone = ((null, 0, null, null)::Nil).toDF("mutationId", "count", "cellParams", "ordering")

    clones.sparkSession.sparkContext.setJobGroup("muller snapshot", "muller plot data snapshot")
    clones.
      join(broadcast(mutationOrder), Seq("mutationId"), "outer").
      unionByName(defaultClone).
      groupBy("ordering").
      agg(
        sum(coalesce(col("count"), lit(0))).as("count")
      ).
      orderBy("ordering").
      drop("ordering").
      as(Encoders.scalaLong).
      collect()
  }



}
