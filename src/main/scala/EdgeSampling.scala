import org.apache.spark.graphx.Graph

import scala.util.Random

case class EdgeSampling() extends SamplingAlgorithm{
  def sample(graph: Graph[Int, Int], fraction: Double): Graph[Int, Int] = {
    //graph.cache()
    val sample = graph.edges.sample(false, fraction, Random.nextLong)//.cache()
    Graph.fromEdges[Int, Int](sample, defaultValue=1)
  }
}