package analyzer 

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.PartitionStrategy

import org.apache.spark.rdd.RDD

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

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

    Graph(vertices, edges, noMutation, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)
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

    startingTree.pregel(
      initialMessage, maxIterations, activeDirection
    )(
      vertexProgram, sendMessage, mergeMessage
    )

  }

  def main(args: Array[String]) = {
    if( args.length != 1 )
      throw new RuntimeException("no prefix path given");
    
    val pathPrefix = args(0);
 
    val spark = SparkSession.builder.
      appName("Phylogeny Testing").
      getOrCreate()
    spark.sparkContext.setCheckpointDir(pathPrefix)

    val chronicleEntries = ChronicleLoader.getOrConvertChronicles(spark, pathPrefix)
    spark.sparkContext.setJobGroup("max Time", "computing maximum time")
    val maxTime = Analyzer.getMaxTime(chronicleEntries);    

    val cellTree = Phylogeny.cellTree(chronicleEntries)
    val mutationTree = Phylogeny.mutationTree(cellTree)
    mutationTree.checkpoint

    //mutationTree.vertices.saveAsObjectFile(pathPrefix + "/mutations.object")
    //mutationTree.edges.saveAsObjectFile(pathPrefix + "/mutationEdges.object")
    val lineageTree = Phylogeny.lineage(mutationTree)

    val snapshots = Snapshots.
      getSnapshots(chronicleEntries, maxTime )

    spark.sparkContext.setJobGroup("muller","compute & save muller plot data")
    Analyzer.saveCSV(pathPrefix + "/muller_plot_data", 
      Muller.mullerData(spark, snapshots, lineageTree),
      true);
  }
}