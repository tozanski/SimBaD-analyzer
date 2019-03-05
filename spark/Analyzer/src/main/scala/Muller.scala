package analyzer

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.cume_dist
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession

object Muller{
  def mullerOrder( spark: SparkSession, lineages: Dataset[Ancestry] ): Dataset[(Long,Long)] = {
    import spark.implicits._

    implicit val mutationOrder = new Ordering[Iterable[Long]]{
      override def compare( lhs: Iterable[Long], rhs: Iterable[Long]): Int = {
        var itL = lhs.iterator;
        val itR = rhs.iterator;

        while( true )
        {
          if( !itL.hasNext && !itR.hasNext) return 0;// are same
          if( itL.hasNext && !itR.hasNext)  return +1// rhs is shorter
          if( !itL.hasNext && itR.hasNext) return -1;// lhs is shorter

          val valL = itL.next
          val valR = itR.next

          if( valL < valR ) return +1;
          if( valL > valR ) return -1;
        }
        return 0;
      }
    }

    lineages.
      rdd.
      map( x => (x.mutationId, x.ancestors.toIterable) ).
      sortBy( _._2 ).
      //map( x => (x._1, x._2.toArray) ).
      //toDF("mutationId","ancestors").
      map( _._1).
      toDF("mutationId").
      withColumn("ordering", monotonically_increasing_id).
      as[(Long,Long)]
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

    val orderedMutations: Dataset[(Long, Long)] = mullerOrder(spark, lineages)

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
}