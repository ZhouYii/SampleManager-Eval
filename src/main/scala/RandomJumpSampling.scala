import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}

import scala.collection.mutable.{HashSet, ListBuffer}
import scala.util.Random

case class RandomJumpSampling(sc: SparkContext, jumpProbability: Double = 0.15)
  extends SamplingAlgorithm{
  def sample(graph: Graph[Int, Int], fraction: Double): Graph[Int, Int] = {
    val v = graph.vertices.collect.sorted
    val e = graph.edges.collect
    val vertexCount:Int = v.size
    val sampleSize:Int = math.floor(fraction*vertexCount).toInt
    var vertex = v(Math.abs(Random.nextInt) % vertexCount)
    var vertices = Map(vertex)
    var edge:Edge[Int] = null
    var edges = HashSet[Edge[Int]]()
    var outEdges:ListBuffer[Edge[Int]] = Util.accumulateEdges(e, vertex._1)
    //var outEdges = e.filter(edge => edge.srcId == vertex._1)
    var oeCount:Int = outEdges.size
    var idx:Int = -1
    var vertexSet:HashSet[Long] = HashSet()

    while(vertices.size < sampleSize){

      if(Random.nextDouble < jumpProbability){
        vertex = v(Math.abs(Random.nextInt) % vertexCount)
        vertices += vertex
        vertexSet += vertex._1
        outEdges = Util.accumulateEdges(e, vertex._1)
        //outEdges = e.filter(edge => edge.srcId == vertex._1)
        oeCount = outEdges.size
        // Should we have a continue statement here? As at each iteration
        // we either walk or jump. Since we have jumped, we should now
        // throw the dice again for the next iteration
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
    var vertices = Map(vertex)
    var edge:Edge[Int] = null
    var edges = HashSet[Edge[Int]]()
    var outEdges:ListBuffer[Edge[Int]] = Util.accumulateEdges(e, vertex._1)
    //var outEdges = e.filter(edge => edge.srcId == vertex._1)
    var oeCount:Int = outEdges.size
    var idx:Int = -1
    var vertexSet:HashSet[Long] = HashSet()

    while(edges.size < sampleSize){

      if(Random.nextDouble < jumpProbability){
        vertex = v(Math.abs(Random.nextInt) % vertexCount)
        vertices += vertex
        vertexSet += vertex._1
        outEdges = Util.accumulateEdges(e, vertex._1)
        //outEdges = e.filter(edge => edge.srcId == vertex._1)
        oeCount = outEdges.size
        // Should we have a continue statement here? As at each iteration
        // we either walk or jump. Since we have jumped, we should now
        // throw the dice again for the next iteration
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
