import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.SparkContext._
import scala.util.Random

case class NodeSampling() extends SamplingAlgorithm {

  def sample(graph: Graph[Int, Int], fraction: Double) : Graph[Int, Int] = {
    //graph.cache()
    val sample = graph.vertices.sample(false, fraction, Random.nextLong)//.cache()
    val c = sample.join(
      sample.join(graph.edges.map {
        case e => (e.srcId, e.dstId)
      }).map {
        case (srcId, (u, dstId)) => (dstId, srcId)
      }).map {
      case (dstId, (u, srcId)) => Edge[Int](srcId, dstId, 1)
    }
    //graph.unpersist(false)
    Graph[Int, Int](sample, c)
  }

}