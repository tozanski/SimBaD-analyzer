package analyzer

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, DataFrame, Encoders, SparkSession}

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
    Analyzer.saveCSV(path, result, coalesce = true)

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
