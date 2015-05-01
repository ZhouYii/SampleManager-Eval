import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.graphx._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable
import scala.util.Random


case class CoCitationSampling(q: VertexId) extends SamplingAlgorithm{

  def collectNeighborIdSet[ED](graph: Graph[Int, ED], edgeDirection: EdgeDirection): VertexRDD[Set[VertexId]] = {

    val nbrs = edgeDirection match {
      case EdgeDirection.Either =>
        graph.aggregateMessages[Set[VertexId]](
          ctx => {
            ctx.sendToSrc(Set(ctx.dstId))
            ctx.sendToDst(Set(ctx.srcId))
          },
          (a, b) => a ++ b, TripletFields.All)
      case EdgeDirection.In =>
        graph.aggregateMessages[Set[VertexId]](
          ctx => ctx.sendToDst(Set(ctx.srcId)),
          (a, b) => a ++ b, TripletFields.Src)
      case EdgeDirection.Out =>
        graph.aggregateMessages[Set[VertexId]](
          ctx => ctx.sendToSrc(Set(ctx.dstId)),
          (a, b) => a ++ b, TripletFields.Dst)
      case EdgeDirection.Both =>
        throw new SparkException("collectEdges does not support EdgeDirection.Both. Use" +
          "EdgeDirection.Either instead.")
    }
    graph.vertices.leftJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Set.empty[VertexId])
    }
  } // end of collectNeighbor

  // weighted cocitation sample, retaining all edges from similar vertices
  def sample(graph: Graph[Int, Int], fraction: Double) : Graph[Int, Int] = {
    val neighborSets = collectNeighborIdSet(graph, EdgeDirection.Out)
    var qNeighbors = mutable.HashSet() ++ graph.edges.filter(_.srcId == q).map(_.dstId).collect
    println("Neighbor size: " + qNeighbors.size)
    qNeighbors = qNeighbors += q
    val edges = neighborSets.flatMap{
      case (v, n) => {
        val intersect = n.intersect(qNeighbors)
        val weight = 2.0*intersect.size / (qNeighbors.size - 1 + n.size)
        if(Random.nextDouble() < weight) {
          for(n_ <- n) yield Edge[Int](v, n_, 1)
        }else{
          Array[Edge[Int]]()
        }
      }
    }
    Graph.fromEdges(edges, defaultValue = 1)
  }

  // weighted cocitation sample, retaining only edges between cocited vertices
  def sampleIntersect(graph: Graph[Int, Int]) : Graph[Int, Int] = {
    val neighborSets = collectNeighborIdSet(graph, EdgeDirection.Out)
    var qNeighbors = mutable.HashSet() ++ graph.edges.filter(_.srcId == q).map(_.dstId).collect
    println("Neighbor size: " + qNeighbors.size)
    qNeighbors = qNeighbors += q
    val edges = neighborSets.flatMap{
      case (v, n) => {
        val intersect = n.intersect(qNeighbors)
        val weight = 2.0*intersect.size / (qNeighbors.size - 1 + n.size)
        if(Random.nextDouble() < weight) {
          for(n_ <- intersect) yield Edge[Int](v, n_, 1)
        }else{
          Array[Edge[Int]]()
        }
      }
    }
    Graph.fromEdges(edges, defaultValue = 1)
  }

}

