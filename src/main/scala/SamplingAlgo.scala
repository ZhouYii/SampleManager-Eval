package org.dprg.graphsampling

import org.apache.spark.rdd.RDD
import scala.util.Random
import org.apache.spark.graphx._

import scala.collection.mutable.{HashSet, ListBuffer}

import scala.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

abstract class SamplingAlgorithm{
  def sample(graph: Graph[Int, Int], fraction: Double) : Graph[Int, Int]
}

object SamplingAlgo {
  def EdgeSample(graph: Graph[Int, Int], fraction: Double, sc: SparkContext)
    : Graph[Int, Int] = {

    val sample = graph.edges.sample(false, fraction, Random.nextLong)//.cache()
    Graph.fromEdges[Int, Int](sample, defaultValue=1)
  }

  def LayeredSample(graph: Graph[Int, Int], fraction: Double, sc: SparkContext, 
                    sparse: VertexRDD[Int], dense:VertexRDD[Int])
    : Graph[Int, Int] = {

    // Unweighted Layer Sampling
    val split = Util.partitionByKMeans(graph.degrees)
    val sparse = split._1
    val dense = split._2

    var sampleVertices:VertexRDD[Int] = VertexRDD(sparse.sample(false, fraction, Random.nextLong).union(dense))
    val groupedSrcEdges:RDD[(VertexId, Iterable[Edge[Int]])] = graph.edges.groupBy(e => e.srcId)
    val srcGroup = sampleVertices.leftJoin(groupedSrcEdges){
      (v, vd, u) => u.getOrElse(Iterable())
    }
    val srcEdges:RDD[Edge[Int]] = srcGroup.flatMap{
      case (v, n) => n
    }
    // reduce to only edges with dstId \in sampleVertices
    val groupedDstEdges = srcEdges.groupBy(e => e.dstId)
    val dstGroup = sampleVertices.leftJoin(groupedDstEdges){
      (v, vd, u) => u.getOrElse(Iterable())
    }
    val sampleEdges: RDD[Edge[Int]] = dstGroup.flatMap{
      case (v, n) => n
    }
    // build the graph from the edges
    val g = Graph.fromEdges(sampleEdges, defaultValue = 1)
    g
  }

  def randomWalkSample(graph: Graph[Int, Int], fraction: Double, sc:SparkContext,
                    jumpThreshold: Int = 100000): Graph[Int, Int] = {
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


  def randomJumpSampling(graph: Graph[Int, Int], fraction: Double, 
                      sc: SparkContext, jumpProbability:Double = 0.15)
                      : Graph[Int, Int] = {
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

