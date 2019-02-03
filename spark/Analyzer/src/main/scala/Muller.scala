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
  def mullerOrder( spark: SparkSession, lineageTree: Graph[List[Long], Double] ): DataFrame = {
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

    lineageTree.
      mapVertices( (id,v) => v.reverse.toIterable).
      vertices.
      sortBy( _._2 ).
      toDF("mutationId","lineage").
      withColumn("ordering", monotonically_increasing_id)
  }

  def mullerData( spark: SparkSession, snapshots: DataFrame, lineageTree: Graph[List[Long], Double] ): DataFrame = {
    import spark.implicits._
    
    val orderedMutations = mullerOrder(spark, lineageTree)

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