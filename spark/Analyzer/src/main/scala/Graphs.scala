package analyzer 

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset


object PhylogeneticTree  {
val noPosition = Position(Float.NaN, Float.NaN, Float.NaN)
val noMutation = Mutation(Float.NaN, Float.NaN, Float.NaN, Float.NaN, Float.NaN, Float.NaN)
val noCell = Cell( Double.NaN, Double.NaN, noPosition, 0, noMutation) 

  def cellTree( chronicleEntries: Dataset[ChronicleEntry]): Graph[Cell, Double] = {
    val vertices = chronicleEntries.rdd.map(
      entry => (entry.particleId, entry.toCell)
    )

    val edges = chronicleEntries.rdd.map( 
      entry => Edge(entry.particleId, entry.parentId, entry.birthTime) 
    )

    Graph(vertices, edges, noCell)
  }

  def mutationTree(cellTree: Graph[Cell, Double]): Graph[Mutation, Double] = {
    val mutatingTriplets = cellTree.
      triplets.
      filter(triplet => triplet.srcAttr.mutationId != triplet.dstAttr.mutationId).
      cache

    val vertices = mutatingTriplets.map( t => (t.srcAttr.mutationId, t.srcAttr.mutation) ).
      distinct
    val edges = mutatingTriplets.
      map( t=> Edge(t.srcAttr.mutationId, t.dstAttr.mutationId, t.attr) ).
      distinct

    Graph(vertices, edges, noMutation)
  }

  def ancestors( mutationTree: Graph[Mutation, Double] ) = {
    
  }

}