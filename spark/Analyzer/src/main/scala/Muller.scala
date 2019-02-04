package analyzer

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.cume_dist
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession


object Muller{
  def mullerOrder( spark: SparkSession, lineages: Dataset[(Long, List[Long])] ): Dataset[(Long,Long)] = {
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
      map( x => (x._1, x._2.reverse.toIterable) ).
      sortBy( _._2 ).
      map( _._1 ).
      toDF("mutationId").
      withColumn("ordering", monotonically_increasing_id).
      as[(Long,Long)]
  }

  def mullerData( spark: SparkSession, snapshots: DataFrame, lineages: Dataset[(Long,List[Long])] ): DataFrame = {
    import spark.implicits._

    val orderedMutations = mullerOrder(spark, lineages)

    val mullerCumulatives = snapshots.
      select("mutationId","timePoint").
      join(orderedMutations,"mutationId").
      withColumn("cumeDist", 
        cume_dist over Window.partitionBy("timePoint").orderBy("ordering")).
      dropDuplicates.
      orderBy("ordering","timePoint").
      drop("ordering")

    mullerCumulatives  
  }


}