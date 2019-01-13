package analyzer 

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset


object CellGraph  {
  def makeGraph( chronicleEntries: Dataset[ChronicleEntry]): Graph[Cell, Double] = {
    val vertices = chronicleEntries.rdd.map(
      entry => (entry.particleId, entry.toCell)
    )

    val edges = chronicleEntries.rdd.map( 
      entry => Edge(entry.particle_id, entry.parent_id, entry.birth_time) 
    )

    Graph(vertices, edges)
  }
}