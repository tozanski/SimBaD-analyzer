package analyzer 

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.PartitionStrategy

import org.apache.spark.rdd.RDD

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import org.apache.spark.storage.StorageLevel

object Phylogeny  {
  val noPosition = Position(Float.NaN, Float.NaN, Float.NaN)
  val noMutation = Mutation(Float.NaN, Float.NaN, Float.NaN, Float.NaN, Float.NaN, Float.NaN)
  val noCell = Cell( Double.NaN, Double.NaN, noPosition, 0, noMutation) 

  def cellTree( chronicleEntries: Dataset[ChronicleEntry]): Graph[Cell, Double] = {
    val vertices = chronicleEntries.rdd.map(
      entry => (entry.particleId, entry.toCell)
    )

    val edges = chronicleEntries.rdd.map( 
      entry => Edge(entry.parentId, entry.particleId, entry.birthTime) 
    )

    Graph(vertices, edges, noCell)
  }

  def mutationTree(cellTree: Graph[Cell, Double]): Graph[Mutation, Double] = {
    val mutatingTriplets = cellTree.
      triplets.
      filter(triplet => triplet.srcAttr.mutationId != triplet.dstAttr.mutationId)

    val vertices = mutatingTriplets.map( t => (t.dstAttr.mutationId, t.dstAttr.mutation) ).
      distinct
    val edges = mutatingTriplets.
      map( t=> Edge(t.srcAttr.mutationId, t.dstAttr.mutationId, t.attr) ).
      distinct

    Graph(vertices, edges, noMutation)
  }

  def lineage( mutationTree: Graph[Mutation, Double] ): Graph[List[Long], Double] = {
    val startingTree = mutationTree.mapVertices( (id,_) => if(1==id) List(id) else Nil )
  
    val initialMessage: List[Long] = Nil
    val maxIterations: Int = Int.MaxValue
    val activeDirection: EdgeDirection = EdgeDirection.Out

    def vertexProgram( id: Long, currAnc: List[Long], incAnc: List[Long] ): List[Long] = {
      if( incAnc.isEmpty ) // initial message
        currAnc
      else 
        id :: incAnc 
    }

    def sendMessage( triplet: EdgeTriplet[List[Long], Double]): Iterator[(Long, List[Long])] = {
      if( triplet.srcAttr.isEmpty || ! triplet.dstAttr.isEmpty)
        Iterator.empty
      else
        Iterator( (triplet.dstId, triplet.srcAttr) )
    }

    def mergeMessage( lhs: List[Long], rhs: List[Long] ): List[Long] = {
      throw new RuntimeException("message merging should had never occured")
    }

    val lineageTree: Graph[List[Long], Double] = startingTree.pregel(
      initialMessage, maxIterations, activeDirection
    )(
      vertexProgram, sendMessage, mergeMessage
    )

    lineageTree
  }

  def main(args: Array[String]) = {
    if( args.length != 1 )
      throw new RuntimeException("no prefix path given");
    
    val pathPrefix = args(0);
 
    val spark = SparkSession.builder.
      appName("Phylogeny Testing").
      getOrCreate()
    spark.sparkContext.setCheckpointDir(pathPrefix + "/tmp")
    import spark.implicits._

    val chronicleEntries = ChronicleLoader.getOrConvertChronicles(spark, pathPrefix)
    spark.sparkContext.setJobGroup("max Time", "computing maximum time")
    val maxTime = Analyzer.getMaxTime(chronicleEntries);    

    spark.sparkContext.setJobGroup("cellTree", "compute CellTree")
    val cellTree = Phylogeny.cellTree(chronicleEntries)

    spark.sparkContext.setJobGroup("mutationDataframe", "save mutation Dataframe")
    Phylogeny.mutationTree(cellTree).
      triplets.
      map( t => (t.dstId, t.srcId, t.dstAttr, t.attr ) ).
      toDF("id", "parentId", "mutation", "timeFirst").
      write.
      mode("overwrite").
      parquet(pathPrefix + "/mutationTree.parquet")

    val mutationDS = spark.
      read.
      parquet(pathPrefix + "/mutationTree.parquet").
      as[(Long, Long, Mutation, Double)].
      repartition(100,col("id"))
        
    val mutationTree = Graph(
      mutationDS.map(x => (x._1, x._3)).rdd,
      mutationDS.map(x => Edge(x._2, x._1, x._4)).rdd,
      noMutation,
      StorageLevel.DISK_ONLY,
      StorageLevel.DISK_ONLY
    )

    spark.sparkContext.setJobGroup("lineage","phylogeny lineage")
    Phylogeny.lineage(mutationTree).
      vertices.
      toDF("id","lineage").
      write.
      mode("overwrite").
      parquet(pathPrefix + "/lineages.parquet")

    val lineages = spark.
      read.
      parquet(pathPrefix + "/lineages.parquet").
      as[(Long,List[Long])].
      repartition(100, col("id"))

    spark.sparkContext.setJobGroup("muller","compute & save muller plot data")
    Analyzer.saveCSV(pathPrefix + "/muller_plot_data", 
      Muller.mullerData(spark, chronicleEntries, lineages, maxTime, 100),
      true);
  }
}