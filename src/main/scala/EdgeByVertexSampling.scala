import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.util.Random

case class EdgeByVertexSampling() extends SamplingAlgorithm{

  def reservoirSample(A: Array[VertexId], fraction: Double): Array[VertexId] = {
    val n: Int = A.size
    val k: Int = math.round(fraction*n).toInt
    // to retain all vertices, swap the above line with the one below
    //val k: Int = math.ceil(fraction*n).toInt
    var B: Array[VertexId] = Array.ofDim(k)
    for(i <- 0 to k-1){
      B(i) = A(i)
    }
    for(i <- k to n-1){
      val j: Int = Random.nextInt(i+1)
      if(j <= k-1){
        B(j) = A(i)
      }
    }
    B
  }

  def sample(graph: Graph[Int, Int], fraction: Double) : Graph[Int, Int] = {
    graph.cache()
    graph.unpersist(false)
    var mapRDD: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Out)//.cache()
    //val mapRDD = graph.edges.groupBy(e => e.srcId)
    ///
    val edges:RDD[Edge[Int]] = mapRDD.flatMap{
      case (v, n) => {
        for(n_ <- reservoirSample(n, fraction)) yield Edge[Int](v, n_, 1)
      }
    }//.cache()
    ///
    Graph.fromEdges[Int, Int](edges, defaultValue = 1)
  }
}
