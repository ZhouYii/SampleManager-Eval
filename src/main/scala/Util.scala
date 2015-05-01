package org.dprg.graphsampling
import java.nio.ByteBuffer
import java.text.DateFormat._
import java.util.{Locale, Date}

import org.apache.hadoop.util.bloom.Key
import org.apache.hadoop.util.bloom.BloomFilter
import org.apache.spark.SparkException
import org.apache.spark.graphx._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.PairRDDFunctions

import scala.collection.{mutable, immutable}
import scala.collection.mutable.{HashSet, ListBuffer}
import scala.reflect.ClassTag
import scala.util.Random

object Util {
  /*
  def createFilter(m: Int, k: Int, t: Int): GSBloomFilter = {
    var bf = new GSBloomFilter
    bf.setFilter(m, k, t)
    bf
  }
  */

  /*
  def genBloomFilter[VD, ED](g: Graph[VD, ED], m: Int, k: Int, t: Int): GSBloomFilter = {
    val bf: GSBloomFilter = g.vertices.aggregate(createFilter(m, k, t))(//new GSBloomFilter())(
      (acc,v) => {
        acc.add(v._1)
        acc
      },
      (acc1, acc2) => {
        acc1.filter.or(acc2.filter)
        acc1
      })
    bf
  }
  */

  def getDate(): String = {
    (new Date).toString
  }
  def exponentialDecay(value:Double, t:Int, lambda:Double) : Double = {
    return value*math.exp(-1.0*lambda*t)
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

  def maxPairArray(A: Array[(VertexId, Int)]): Int = {
    var max = -1
    for((v, u) <- A){
      if(u > max){
        max = u
      }
    }
    max
  }


  def partitionByKMeans(degrees: VertexRDD[Int]): (VertexRDD[Int], VertexRDD[Int]) = {
    val data = degrees.map(v => Vectors.dense(v._2))
    // data.collect.foreach(println)
    val numIterations = 30
    val clusters = KMeans.train(data, 2, numIterations)
    //clusters.clusterCenters.foreach(println)
    val firstCentroid = clusters.clusterCenters.apply(0).apply(0)
    val secondCentroid = clusters.clusterCenters.apply(1).apply(0)
    var sparseCentroid: Double = 0.0
    var denseCentroid: Double = 0.0
    if(firstCentroid < secondCentroid){
      sparseCentroid = firstCentroid
      denseCentroid = secondCentroid
    }else{
      sparseCentroid = secondCentroid
      denseCentroid = firstCentroid
    }
    val sparse:VertexRDD[Int] = degrees.filter(v => {
      if((v._2 - sparseCentroid).abs < (v._2 - denseCentroid).abs){
        true
      }else {
        false
      }
    })
    val dense: VertexRDD[Int] = VertexRDD(degrees.subtract(sparse))
    (sparse, dense)
  }

  def collectNeighborDegrees[ED](graph: Graph[Int, ED], edgeDirection: EdgeDirection): VertexRDD[Array[(VertexId, Int)]] = {

    val nbrs = edgeDirection match {
      case EdgeDirection.Either =>
        graph.aggregateMessages[Array[(VertexId,Int)]](
          ctx => {
            ctx.sendToSrc(Array((ctx.dstId, ctx.dstAttr)))
            ctx.sendToDst(Array((ctx.srcId, ctx.srcAttr)))
          },
          (a, b) => a ++ b, TripletFields.All)
      case EdgeDirection.In =>
        graph.aggregateMessages[Array[(VertexId,Int)]](
          ctx => ctx.sendToDst(Array((ctx.srcId, ctx.srcAttr))),
          (a, b) => a ++ b, TripletFields.Src)
      case EdgeDirection.Out =>
        graph.aggregateMessages[Array[(VertexId,Int)]](
          ctx => ctx.sendToSrc(Array((ctx.dstId, ctx.dstAttr))),
          (a, b) => a ++ b, TripletFields.Dst)
      case EdgeDirection.Both =>
        throw new SparkException("collectEdges does not support EdgeDirection.Both. Use" +
          "EdgeDirection.Either instead.")
    }
    graph.vertices.leftJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Array.empty[(VertexId, Int)])
    }
  } // end of collectNeighbor



  /*
  Accumulate all edges with the target source id

  @param e - the sorted edge array
  @param target - the target source vertex id
  @return a ListBuffer with all edges
   */
  def accumulateEdges(e:Array[Edge[Int]],
                      target:VertexId) : ListBuffer[Edge[Int]] = {
    val idx = binarySearchE(e, target)(0, e.size-1)
    var outEdges: ListBuffer[Edge[Int]] = ListBuffer()
    if(idx == -1){
      return outEdges
    }
    outEdges.append(e(idx))
    var tIdx = idx+1
    var edge:Edge[Int] = null
    // get upper edges
    while(tIdx < e.size){
      edge = e(tIdx)
      if(edge.srcId == target){
        outEdges.append(edge)
        tIdx += 1
      }else{
        tIdx = e.size
      }
    }
    // get lower edges
    tIdx = idx-1
    while(tIdx > -1){
      edge = e(tIdx)
      if(edge.srcId == target){
        outEdges.append(edge)
        tIdx -= 1
      }else{
        tIdx = -1
      }
    }
    outEdges
  }

  def binarySearchV(list: Array[(VertexId, Int)], target: VertexId)
                   (start: Int=0, end: Int=list.length-1): Int = {
    if (start>end) return -1
    val mid = start + (end-start+1)/2
    if (list(mid)._1==target)
      return mid
    else if (list(mid)._1>target)
      return binarySearchV(list, target)(start, mid-1)
    else
      return binarySearchV(list, target)(mid+1, end)
  }

  def binarySearchE(list: Array[Edge[Int]], target: VertexId)
                   (start: Int=0, end: Int=list.length-1): Int = {
    if (start>end) return -1
    val mid = start + (end-start+1)/2
    if (list(mid).srcId==target)
      return mid
    else if (list(mid).srcId>target)
      return binarySearchE(list, target)(start, mid-1)
    else
      return binarySearchE(list, target)(mid+1, end)
  }

  def geometricSample(param: Double) : Int = {
    var num = 1
    while(Random.nextDouble <= param) {
      num += 1
    }
    num
  }

  def sortBySrc(a:Array[Edge[Int]]): Array[Edge[Int]] = {
    if (a.length < 2) a
    else {
      val pivot = a(a.length / 2).srcId
      // 'L'ess, 'E'qual, 'G'reater
      val partitions = a.groupBy( (e:Edge[Int]) => {
        if (e.srcId < pivot)
          'L'
        else if (e.srcId > pivot)
          'G'
        else
          'E'
      })

      var sortedAccumulator: Array[Edge[Int]] = Array()
      List('L', 'E', 'G').foreach((c:Char) => {
        if (partitions.contains(c)) {
          sortedAccumulator = sortedAccumulator ++ partitions(c)
        }
      })
      sortedAccumulator
    }
  }

  /*
    @param sc - the Spark context
    @param v - the sorted vertex array
    @param e - the sorted edge array
    @param fraction - the fraction of total vertices to sample
    @return the sample graph
     */
  def randomJumpSample(v:Array[(VertexId, Int)],
                       e:Array[Edge[Int]],
                       fraction:Double): (Map[VertexId, Int], HashSet[Edge[Int]]) = { //(Graph[Int, Int], HashSet[Long]) = {
  val vertexCount:Int = v.size
    val sampleSize:Int = math.floor(fraction*vertexCount).toInt
    var vertex = v(Math.abs(Random.nextInt) % vertexCount)
    var vertices = Map(vertex)
    var edge:Edge[Int] = null
    var edges = HashSet[Edge[Int]]()
    //var outEdges:ListBuffer[Edge[Int]] = accumulateEdges(e, vertex._1)
    var outEdges = e.filter(edge => edge.srcId == vertex._1)
    var oeCount:Int = outEdges.size
    var idx:Int = -1
    var vertexSet:HashSet[Long] = HashSet()

    while(vertices.size < sampleSize){
      if(Random.nextDouble < 0.15){
        vertex = v(Math.abs(Random.nextInt) % vertexCount)
        vertices += vertex
        vertexSet += vertex._1
        //outEdges = accumulateEdges(e, vertex._1)
        outEdges = e.filter(edge => edge.srcId == vertex._1)
        oeCount = outEdges.size
        // Should we have a continue statement here? As at each iteration
        // we either walk or jump. Since we have jumped, we should now
        // throw the dice again for the next iteration
      }

      if(oeCount > 0){
        edge = outEdges(Math.abs(Random.nextInt) % oeCount)
        edges += edge
        //idx = binarySearchV(v, edge.dstId)(0, vertexCount-1)
        //vertex = v(idx)
        vertex = v.find(element => element._1 == edge.dstId).get
        vertices += vertex
        vertexSet += vertex._1
        //outEdges = accumulateEdges(e, vertex._1)
        outEdges = e.filter(edge => edge.srcId == vertex._1)
        oeCount = outEdges.size
      }

    }
    (vertices, edges)
    //(Graph(sc.parallelize(vertices.toSeq), sc.parallelize(edges.toSeq)), vertexSet)
  }
}




