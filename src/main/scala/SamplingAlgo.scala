package org.dprg.graphsampling
import scala.util.Random
import org.apache.spark.graphx._

abstract class SamplingAlgorithm{
  def sample(graph: Graph[Int, Int], fraction: Double) : Graph[Int, Int]
}

object SamplingAlgo {
  def EdgeSample(graph: Graph[Int, Int], fraction: Double): Graph[Int, Int] = {
    val sample = graph.edges.sample(false, fraction, Random.nextLong)//.cache()
    Graph.fromEdges[Int, Int](sample, defaultValue=1)
  }
}

