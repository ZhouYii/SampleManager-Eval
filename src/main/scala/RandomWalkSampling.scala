import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}

import scala.collection.mutable.{HashSet, ListBuffer}
import scala.util.Random

case class RandomWalkSampling(sc: SparkContext, jumpThreshold:Long = 100000)
  extends SamplingAlgorithm{
  /*
@param sc - the Spark context
@param v - the sorted vertex array
@param e - the sorted edge array
@param fraction - the fraction of total vertices to sample
@return the sample graph
*/
  def sample(graph: Graph[Int, Int], fraction: Double): Graph[Int, Int] = {
    val v = graph.vertices.collect.sorted
    val e = graph.edges.collect

    val vertexCount:Int = v.size
    val sampleSize:Int = math.floor(fraction*vertexCount).toInt
    var vertex = v(Math.abs(Random.nextInt) % vertexCount)
    var homeVertex = vertex
    var vertices = Map(vertex)
    var edge:Edge[Int] = null
    var edges = HashSet[Edge[Int]]()
    var outEdges:ListBuffer[Edge[Int]] = Util.accumulateEdges(e, vertex._1)
    //var outEdges = e.filter(edge => edge.srcId == vertex._1)
    var oeCount:Int = outEdges.size
    var idx:Int = -1
    var vertexSet:HashSet[Long] = HashSet()
    var iterationCounter = 0L

    while(vertices.size < sampleSize){
      iterationCounter += 1

      // Sampling paper defines RW Sampling in which at each iteration we
      // go back to the home vertex with some probability
      if(Random.nextDouble < 0.15) {
        vertex = homeVertex
        outEdges = Util.accumulateEdges(e, vertex._1)
        //outEdges = e.filter(edge => edge.srcId == vertex._1)
        oeCount = outEdges.size
      }

      if (iterationCounter % jumpThreshold == 0) {
        if (vertex == homeVertex) {
          vertex = v(Math.abs(Random.nextInt) % vertexCount)
          vertices += vertex
          homeVertex = vertex
          outEdges = Util.accumulateEdges(e, vertex._1)
          //outEdges = e.filter(edge => edge.srcId == vertex._1)
          //homeEdges = outEdges
          oeCount = outEdges.size
        } else {
          homeVertex = vertex
        }
      }

      if(oeCount > 0){
        edge = outEdges(Math.abs(Random.nextInt) % oeCount)
        edges += edge
        idx = Util.binarySearchV(v, edge.dstId)(0, vertexCount-1)
        vertex = v(idx)
        //vertex = v.find(element => element._1 == edge.dstId).get
        vertices += vertex
        vertexSet += vertex._1
        outEdges = Util.accumulateEdges(e, vertex._1)
        //outEdges = e.filter(edge => edge.srcId == vertex._1)
        oeCount = outEdges.size
      }

    }
    Graph(sc.parallelize(vertices.toSeq), sc.parallelize(edges.toSeq))
  }

  def sampleEdges(graph: Graph[Int, Int], fraction: Double): Graph[Int, Int] = {
    val v = graph.vertices.collect.sorted
    val e = graph.edges.collect

    val vertexCount:Int = v.size
    val edgeCount:Int = e.size
    val sampleSize:Int = math.floor(fraction*edgeCount).toInt
    var vertex = v(Math.abs(Random.nextInt) % vertexCount)
    var homeVertex = vertex
    var vertices = Map(vertex)
    var edge:Edge[Int] = null
    var edges = HashSet[Edge[Int]]()
    var outEdges:ListBuffer[Edge[Int]] = Util.accumulateEdges(e, vertex._1)
    //var outEdges = e.filter(edge => edge.srcId == vertex._1)
    var oeCount:Int = outEdges.size
    var idx:Int = -1
    var vertexSet:HashSet[Long] = HashSet()
    var iterationCounter = 0L

    while(edges.size < sampleSize){
      iterationCounter += 1

      // Sampling paper defines RW Sampling in which at each iteration we
      // go back to the home vertex with some probability
      if(Random.nextDouble < 0.15) {
        vertex = homeVertex
        outEdges = Util.accumulateEdges(e, vertex._1)
        //outEdges = e.filter(edge => edge.srcId == vertex._1)
        oeCount = outEdges.size
      }

      if (iterationCounter % jumpThreshold == 0) {
        if (vertex == homeVertex) {
          vertex = v(Math.abs(Random.nextInt) % vertexCount)
          vertices += vertex
          homeVertex = vertex
          outEdges = Util.accumulateEdges(e, vertex._1)
          //outEdges = e.filter(edge => edge.srcId == vertex._1)
          //homeEdges = outEdges
          oeCount = outEdges.size
        } else {
          homeVertex = vertex
        }
      }

      if(oeCount > 0){
        edge = outEdges(Math.abs(Random.nextInt) % oeCount)
        edges += edge
        idx = Util.binarySearchV(v, edge.dstId)(0, vertexCount-1)
        vertex = v(idx)
        //vertex = v.find(element => element._1 == edge.dstId).get
        vertices += vertex
        vertexSet += vertex._1
        outEdges = Util.accumulateEdges(e, vertex._1)
        //outEdges = e.filter(edge => edge.srcId == vertex._1)
        oeCount = outEdges.size
      }

    }
    Graph(sc.parallelize(vertices.toSeq), sc.parallelize(edges.toSeq))
  }

}