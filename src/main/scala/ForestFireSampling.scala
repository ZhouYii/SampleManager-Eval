import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import scala.collection.mutable.{Queue, HashSet}
import scala.util.Random

case class ForestFireInducedSampling(sc: SparkContext, geoParam: Double = 0.7)
  extends SamplingAlgorithm {

  def sample(graph: Graph[Int, Int], fraction: Double): Graph[Int, Int] = {
    var g = graph
    var e = Util.sortBySrc(g.edges.toArray)
    val targetVertexCount = (graph.vertices.count() * fraction).toInt
    var seedVertices = graph.vertices
      .sample(false, fraction, targetVertexCount)
      .toArray.iterator
    //println( s"Sampling Start. Target : ${targetVertexCount} " )
    var sampledVertices: HashSet[VertexId] = HashSet()
    var burnQueue: Queue[VertexId] = Queue()

    while (sampledVertices.size < targetVertexCount) {
      val seedVertex = seedVertices.next
      sampledVertices += seedVertex._1
      burnQueue += seedVertex._1
      while (burnQueue.size > 0 ){
        val vertexId = burnQueue.dequeue()
        val numToSample = Util.geometricSample(geoParam)
        val edgeCandidates = Util.accumulateEdges(e, vertexId)
        //val edgeCandidates = e.filter(edge => edge.srcId == vertexId)
        val burnCandidate = sc.parallelize(edgeCandidates).filter( (e:Edge[Int]) => {
          // New Vertex
          !sampledVertices.contains(e.dstId)
        })

        val burnFraction = math.min(numToSample.toDouble / burnCandidate.count.toDouble, 1.0)
        val burnEdges = burnCandidate.sample(false, burnFraction, Random.nextLong)
        //println(s"Geometric Num:${numToSample}, Burned ${burnEdges.count}")

        val neighborVertexIds = burnEdges.map((e:Edge[Int]) => e.dstId)
        sampledVertices = sampledVertices ++ neighborVertexIds.toArray
        burnQueue = burnQueue ++ neighborVertexIds.toArray

        if (sampledVertices.size > targetVertexCount) {
          burnQueue.dequeueAll((v:VertexId) => true)
        }
      }
    }
    val vertex: Seq[(VertexId, Int)] = sampledVertices.map((v:VertexId) => (v,1)).toSeq
    val edges = graph.edges.filter(e => sampledVertices.contains(e.srcId) &&
      sampledVertices.contains(e.dstId))
    Graph(sc.parallelize(vertex), edges)
  }

}

case class ForestFireSampling(sc: SparkContext, geoParam: Double = 0.7)
  extends SamplingAlgorithm {

  def sample(graph: Graph[Int, Int], fraction: Double): Graph[Int, Int] = {
    var g = graph
    var e = Util.sortBySrc(g.edges.toArray)
    val targetVertexCount = (graph.vertices.count() * fraction).toInt
    var seedVertices = graph.vertices
      .sample(false, fraction, targetVertexCount)
      .toArray.iterator
    println( s"Sampling Start. Target : ${targetVertexCount} " )
    var sampledVertices: HashSet[VertexId] = HashSet()
    var sampledEdges: HashSet[Edge[Int]] = HashSet()
    var burnQueue: Queue[VertexId] = Queue()

    while (sampledVertices.size < targetVertexCount) {
      val seedVertex = seedVertices.next
      println(s"Adding from seed set : ${seedVertex._1}")
      sampledVertices += seedVertex._1
      burnQueue += seedVertex._1
      while (burnQueue.size > 0 ){
        val vertexId = burnQueue.dequeue()
        val numToSample = Util.geometricSample(geoParam)
        //val edgeCandidates = accumulateEdges(e, vertexId)
        val edgeCandidates = e.filter(edge => edge.srcId == vertexId)
        val burnCandidate = sc.parallelize(edgeCandidates).filter( (e:Edge[Int]) => {
          // New Vertex
          !sampledVertices.contains(e.dstId) &&
            // New Edge
            !sampledEdges.contains(e)
        })

        val burnFraction = numToSample.toDouble / burnCandidate.count.toDouble
        val burnEdges = burnCandidate.sample(false, burnFraction, Random.nextLong)
        //println(s"Geometric Num:${numToSample}, Burned ${burnEdges.count}")

        val neighborVertexIds = burnEdges.map((e:Edge[Int]) => e.dstId)
        sampledVertices = sampledVertices ++ neighborVertexIds.toArray
        sampledEdges = sampledEdges ++ burnEdges.toArray
        burnQueue = burnQueue ++ neighborVertexIds.toArray

        if (sampledVertices.size > targetVertexCount) {
          burnQueue.dequeueAll((v:VertexId) => true)
        }
      }
    }
    val vertex: Seq[(VertexId, Int)] = sampledVertices.map((v:VertexId) => (v,1)).toSeq
    val parallel = sc.parallelize(vertex).count()
    Graph(sc.parallelize(vertex), sc.parallelize(sampledEdges.toSeq))
  }

}