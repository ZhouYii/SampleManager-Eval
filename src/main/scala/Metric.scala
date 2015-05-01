package org.dprg.graphsampling

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.lib.LabelPropagation
import scala.util.Random


object Metrics {
  implicit val vertexOrdering = new Ordering[(VertexId, Double)] {
    override def compare(a: (VertexId, Double), b: (VertexId, Double)) = a._2.compare(b._2)
  }

  def min(nums: Int*): Int = nums.min

  def minEditDistance(target:Array[VertexId],
                      source:Array[VertexId]) : Int = {
    val N = target.length
    val M = source.length
    val d: Array[Array[Int]] = Array.ofDim(N + 1, M + 1)
    for(i <- 0 to N) d(i)(0) = i
    for(j <- 0 to M) d(0)(j) = j
    for(i <- 1 to N; j <- 1 to M){
      d(i)(j) = min(
        d(i-1)(j) + 1,
        d(i)(j-1) + 1,
        d(i-1)(j-1) + (if(target(i-1) == source(j-1)) 0 else 2)
      )
    }
    d(N)(M)
  }

  def pageRankEditMetric(groundTruth: Graph[Double,Double],
                        sampleResults: Graph[Double,Double],
                        num: Int = 100) : Int = {
    val sortedOriginal = groundTruth.vertices.top(num)(vertexOrdering).map(_._1)
    val sortedSample = sampleResults.vertices.top(num)(vertexOrdering).map(_._1)
    minEditDistance(sortedOriginal, sortedSample)
  }

  def pageRankIntersectMetric(groundTruth: Graph[Double,Double],
                              sampleResults: Graph[Double,Double],
                              num: Int = 100) : Int = {
  val sortedOriginal = groundTruth.vertices.top(num)(vertexOrdering).map(_._1)
  val sortedSample = sampleResults.vertices.top(num)(vertexOrdering).map(_._1)
  val intersect = (Set() ++ sortedSample) intersect (Set() ++ sortedOriginal)
  intersect.size
  }
}
 
