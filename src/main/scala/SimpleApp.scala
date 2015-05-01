package org.dprg.graphsampling
import com.google.common.hash.BloomFilter
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{HashSet, ListBuffer, Map, ArraySeq}
import scala.util.Random

import scala.reflect.ClassTag
import scala.util.Random
import org.apache.hadoop.util.hash.Hash
import scala.collection.mutable.HashSet


import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.lib.LabelPropagation
/*

~/PredictionIO/vendors/spark-1.2.0/bin/spark-submit \
  --class "SimpleApp" \
  --master local[8] \
  --driver-memory 10G \
  target/scala-2.10/graph-sampling_2.10-0.1-SNAPSHOT.jar
 */

// Serializing graph objects
import java.io._

object SimpleApp {
  implicit val vertexOrdering = new Ordering[(VertexId, Double)] {
    override def compare(a: (VertexId, Double), b: (VertexId, Double)) = a._2.compare(b._2)
  }

  def pageRankErrorAverage(sc: SparkContext,
                           origValueGraph: Graph[Double, Double],
                           sampleValueGraph: Graph[Double, Double]) : Double = {
    sampleValueGraph.vertices.repartition(origValueGraph.vertices.partitions.size)

    //origValueGraph.vertices.saveAsObjectFile("./tmp_orig")
    val origRDD : RDD[(VertexId, Double)] = sc.objectFile("./tmp_orig")
    val origVRDD = VertexRDD(origRDD)
    //sampleValueGraph.vertices.saveAsObjectFile("./tmp_sample1")
    val sampleRDD : RDD[(VertexId, Double)] = sc.objectFile("./tmp_sample1")
    val sampleVRDD = VertexRDD(sampleRDD)

    val result = origVRDD.leftJoin(sampleVRDD)
      //{(id, correctComponent, sampleComponent) => (correctComponent, sampleComponent.getOrElse(-1.0))}.
    { (id, correctComponent, sampleComponent) => {
      val sampleComp = sampleComponent.getOrElse(-1.0)
      if (sampleComp == -1.0) {
        0.0
      } else {
        (correctComponent - sampleComp).abs
      }
    }
    }.reduce((u, v) => (0L, u._2 + v._2))._2
    return result/sampleValueGraph.vertices.count()
  }

  def main(args: Array[String]) {
    println("Main")
    if (args.size < 6) {
      println("Arguments not enough")
      return
    }

    val graphPath = args(0)
    val sampleType = args(1)
    val sampleFrac = args(2).toDouble
    val queryAlgo = args(3)
    val stitchNumGraph = args(4).toInt
    val stitchStrategy = args(5)
    val jobName = s"GS-${graphPath}-$sampleType-${sampleFrac.toString()}-$queryAlgo-${stitchNumGraph.toString()}-${stitchStrategy}"
    println(jobName)

    val conf = new SparkConf()
      .setAppName(jobName)
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    /*
    // Time Evaluation
    val t0 = System.nanoTime()
    val edgeFile = "data/edge_list.txt"
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, edgeFile).cache()
    val s = EdgeSampling().sample(graph, 0.15)
    s.pageRank(0.001)
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    */

    val g = loadGraph(sc, queryAlgo, graphPath)

    val samples = getSamples(g, sampleType, sampleFrac, stitchNumGraph)
    val aggregateResult = runQuery(samples, queryAlgo, stitchStrategy)
    val groundTruth = runQuery(List(g), queryAlgo, "None")

    // PageRank
    runEval[Graph[Double,Double]](
        aggregateResult.asInstanceOf[Graph[Double,Double]],
        groundTruth.asInstanceOf[Graph[Double,Double]],
        stitchNumGraph,
        sampleType,
        sampleFrac,
        queryAlgo,
        stitchStrategy)
    sc.stop()
  }

  def loadGraph(sc: SparkContext, queryAlgo: String, graphDataPath: String) = {
    queryAlgo match {
      case "triangleCount" => 
        GraphLoader.edgeListFile(sc, graphDataPath, canonicalOrientation=true)
      case _ =>
        GraphLoader.edgeListFile(sc, graphDataPath)
    }
  }

  def runEval[T](aggregateResult: T, groundTruth: T, numSamples: Int, 
                 sampleType: String, sampleFrac: Double, queryAlgo: String,
                 stitchStrategy: String) = {

    queryAlgo match {
      case "pageRank" => {
        val result = aggregateResult.asInstanceOf[Graph[Double,Double]]
        val truth = groundTruth.asInstanceOf[Graph[Double,Double]]

        var metricName = "Edit Distance"
        var metricScore = Metrics.pageRankEditMetric(truth, result)
        println(s"$sampleType,$sampleFrac,$queryAlgo,$metricName,$metricScore,$numSamples,$stitchStrategy")

        metricName = "Intersection"
        metricScore = Metrics.pageRankIntersectMetric(truth, result)
        println(s"$sampleType,$sampleFrac,$queryAlgo,$metricName,$metricScore,$numSamples,$stitchStrategy")
      }
    }

  }

  def runStitching[T](results: List[T],
    queryAlgo: String,
    stitchStrategy: String) : T = {
    
    queryAlgo match {
      case "pageRank" => 
        stitchStrategy match {
          case _ => results.apply(0).asInstanceOf[T]
        }
    }
  }

  def runQuery(samples: List[Graph[Int,Int]],
    queryAlgo: String,
    stitchStrategy: String) = {

    queryAlgo match {
      case "triangleCount" => {
        val results = samples.map(g => g.triangleCount())
        runStitching[Graph[Int,Int]](results, queryAlgo, "None")
      }

      case "pageRank" => {
        val results = samples.map(g => g.pageRank(0.001))
        runStitching[Graph[Double,Double]](results, queryAlgo, "None")
        // vertex, score
        // result.apply(0).vertices.foreach(println)
      }

      case "connectedComponents" => {
        val result = samples.map(g => g.connectedComponents())
        result

        //val result = samples.map(g => ccGetNumComponents(g.connectedComponents()))

        // stitching can be good. if a,b are in different components in one
        // sample, and a,b are in sample component in a different sample, then
        // you can infer a,b are in same sample

      }

    }

  }

  def getSamples(g: Graph[Int,Int],
    sampleType: String,
    sampleFrac: Double,
    numSamples: Int) = {

    val numList = List.range(0, numSamples)
    sampleType match {
      /*
      case "layered" => {

      }
      */

      case "Edge Sample" => {
        numList.map(x => SamplingAlgo.EdgeSample(g, sampleFrac))
      }

      case _ => {
        numList.map(x => g)
      }
    }
  }

  def ccGetNumComponents(g : Graph[VertexId,Int]) = {
    val hs = new scala.collection.mutable.HashSet
    val ids = g.vertices.map(x => x._2).distinct
    ids.count()
  }

}
