import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.lib.LabelPropagation
import scala.util.Random


object EvalFuncs {
  // sc: SparkContext, directory: String
  implicit val vertexOrdering = new Ordering[(VertexId, Double)] {
    override def compare(a: (VertexId, Double), b: (VertexId, Double)) = a._2.compare(b._2)
  }

  def pageRankIntersectMetric(groundTruth: Graph[Double,Double],
                              sampleResults: Graph[Double, Double],
                              num: Int = 100) : Int = {
    val sortedOriginal = groundTruth.vertices.top(num)(vertexOrdering).map(_._1)
    val sortedSample = sampleResults.vertices.top(num)(vertexOrdering).map(_._1)
    val intersect = (Set() ++ sortedSample) intersect (Set() ++ sortedOriginal)
    intersect.size
  }

  /*
  def runR(metric: MetricType.MetricType,
          sampleResults: AlgorithmResults,
          other: Any = null) : Double = {
    metric match{

      case MetricType.TRIANGLE_COUNT => {
        triangleCountMetric(groundTruth, sampleResults)
      }
      case MetricType.SHORTEST_PATHS => {
        shortestPathsMetric(groundTruth, sampleResults, other.asInstanceOf[Seq[VertexId]])
      }
      case MetricType.LABEL_PROPAGATION => {
        labelPropagationMetric(groundTruth, sampleResults)
      }
    }
  }
  */

  def pageRankEditGivenMetric(sortedOriginal: Array[VertexId],
                               sortedSample: Array[VertexId]) : Int = {
    Util.minEditDistance(sortedOriginal, sortedSample)
  }

  def pageRankEditMetric(groundTruth: Graph[Double,Double],
                        sampleResults: Graph[Double,Double],
                        num: Int = 100) : Int = {
    val sortedOriginal = groundTruth.vertices.top(num)(vertexOrdering).map(_._1)
    val sortedSample = sampleResults.vertices.top(num)(vertexOrdering).map(_._1)
    Util.minEditDistance(sortedOriginal, sortedSample)
  }

  def connectedComponentsMetric(groundTruth: Graph[VertexId, Int],
                                sampleResults: Graph[VertexId, Int]) : Double = {
    return valueError(groundTruth, sampleResults)
  }

  def stronglyConnectedComponentsMetric(groundTruth: Graph[VertexId, Int],
                                        sampleResults: Graph[VertexId, Int]) : Double = {
    return valueError(groundTruth, sampleResults)
  }

  def triangleCountMetric(groundTruth: AlgorithmResults,
                          sampleResults: AlgorithmResults) : Double = {
    return triangleCountErrorAverage(groundTruth.TC, sampleResults.TC)
  }

  def shortestPathsMetric(groundTruth: Graph[scala.collection.immutable.Map[Long,Int], Int],
                          sampleResults: Graph[scala.collection.immutable.Map[Long,Int], Int],
                          landmarks: Seq[VertexId]) : Double = {
    return shortestPathsError(groundTruth, sampleResults, landmarks)
  }

  // The description of the label propagation algorithm can be found at:
  // https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/lib/LabelPropagation.scala
  def labelPropagationMetric(groundTruth: Graph[VertexId, Int],
                             sampleResults: Graph[VertexId, Int]) : Double = {
    return valueError(groundTruth, sampleResults)
  }

  def pageRankAverageMetric(sc: SparkContext,
                            groundTruth: Graph[Double,Double],
                             sampleResults: Graph[Double,Double]) : Double = {
    return pageRankErrorAverage(sc, groundTruth, sampleResults)
  }

  // The error for connected components and label propagation is defined as
  // the fraction of incorrect component values determined by the algorithm
  // for the graph sample.
  def valueError(origValueGraph: Graph[VertexId, Int],
                 sampleValueGraph: Graph[VertexId, Int]) : Double = {
    val valueTuple = origValueGraph.vertices.leftJoin(sampleValueGraph.vertices)
    {(id, correctComponent, sampleComponent) => (correctComponent, sampleComponent.getOrElse(-1))}.cache()
    val accurateVertices = valueTuple.filter(element => element._2._1 == element._2._2).cache()
    return 1 - (accurateVertices.count().toDouble/sampleValueGraph.vertices.count())
  }


  // The average error for triangle count is defined as the average of the
  // difference between the actual triangle count values and the triangle count
  // values determined by the triangle count algorithm on the graph sample.
  def triangleCountErrorAverage(origValueGraph: Graph[Int, Int],
                                sampleValueGraph: Graph[Int, Int]) : Double = {
    val valueTuple = origValueGraph.vertices.leftJoin(sampleValueGraph.vertices)
    {(id, correctComponent, sampleComponent) => (correctComponent, sampleComponent.getOrElse(-1))}.
      filter(element => element._2._2 != -1)
    var result = 0.0
    valueTuple.collect.foreach(element => result += (element._2._1 - element._2._2).abs)
    return result/sampleValueGraph.vertices.count()
  }

  // The L1 error for triangle count is defined as the L1 norm of the difference
  // between the actual triangle count values and the triangle count values
  // determined by the triangle count algorithm on the graph sample.
  // The L1 and L2 errors do not make much sense.
  def triangleCountErrorL1(origValueGraph: Graph[Int, Int],
                           sampleValueGraph: Graph[Int, Int]) : Double = {
    val valueTuple = origValueGraph.vertices.leftJoin(sampleValueGraph.vertices)
    {(id, correctComponent, sampleComponent) => (correctComponent, sampleComponent.getOrElse(-1))}.
      filter(element => element._2._2 != -1)

    var result = 0.0
    valueTuple.collect.foreach(element => result += (element._2._1 - element._2._2).abs)
    return result
  }

  // The L2 error for triangle count is defined as the L2 norm of the difference
  // between the actual triangle count values and the triangle count values
  // determined by the triangle count algorithm on the graph sample.
  // The L1 and L2 errors do not make much sense.
  def triangleCountErrorL2(origValueGraph: Graph[Int, Int],
                           sampleValueGraph: Graph[Int, Int]) : Double = {
    val valueTuple = origValueGraph.vertices.leftJoin(sampleValueGraph.vertices)
    {(id, correctComponent, sampleComponent) => (correctComponent, sampleComponent.getOrElse(-1))}.
      filter(element => element._2._2 != -1)

    var result = 0.0
    valueTuple.collect.foreach(element => result += Math.pow((element._2._1 - element._2._2).abs, 2.0))
    return Math.pow(result, 0.5)
  }

  // The error for shortest paths is defined as the average difference between
  // the actual shortest path values to a set of landmarks and the shortest path
  // values determined by the shortest paths algorithm on the graph sample.
  // Interesting Case: If there exists a path to a landmark but for the graph
  // sample, the landmark is unreachable from a vertex, how much penalty should
  // be added for this case ?
  def shortestPathsError(origValueGraph: Graph[scala.collection.immutable.Map[Long,Int], Int],
                         sampleValueGraph: Graph[scala.collection.immutable.Map[Long,Int], Int],
                         landmarks: Seq[VertexId])
  : Double = {

    val valueTuple = origValueGraph.vertices.leftJoin(sampleValueGraph.vertices)
    {(id, correctComponent, sampleComponent) => (correctComponent, sampleComponent.getOrElse(Map[Long, Int]()))}.
      filter(element => element._2._2.size > 0)
    var result = 0.0
    valueTuple.collect.foreach(element => {
      landmarks.foreach { landmark => result += (element._2._1.getOrElse(landmark, 0) - element._2._2.getOrElse(landmark, 0))}
    })
    return result/( sampleValueGraph.vertices.count() * landmarks.size )
  }

  // The average error for this version of page rank is defined as the
  // average of the difference between the actual page rank values and
  // the page rank values determined by the algorithm on the graph sample.
  def pageRankErrorAverage(sc: SparkContext,
                           origValueGraph: Graph[Double, Double],
                           sampleValueGraph: Graph[Double, Double]) : Double = {
    sampleValueGraph.vertices.repartition(origValueGraph.vertices.partitions.size)

    val origRDD : RDD[(VertexId, Double)] = sc.objectFile("./tmp_orig")
    val origVRDD = VertexRDD(origRDD)
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


  /*----------------------------------------------------------------*/

  /*
  def runOrig(metric: MetricType.MetricType,
              groundTruth: AlgorithmResults,
              sample: Graph[Int, Int],
              id: String) : Double = {
    metric match{
      case MetricType.PAGERANK_EDIT => {
        pageRankEditMetricOrig(groundTruth.PR, sample, id)
      }
      case MetricType.PAGERANK_INTERSECT => {
        pageRankEditMetricOrig(groundTruth.PR, sample, id)
      }
      case MetricType.PAGERANK_AVERAGE_ERROR => {
        pageRankMetricWithAverageErrorOrig(sc, groundTruth.PR, sample, id)
      }
      case MetricType.CC => {
        connectedComponentsMetricOrig(groundTruth.CC, sample, id)
      }
      case MetricType.SCC => {
        stronglyConnectedComponentsMetricOrig(groundTruth.SCC, sample, 30, id)
      }
      case MetricType.TRIANGLE_COUNT => {
        triangleCountMetricOrig(groundTruth.TC, sample, id)
      }
      case MetricType.SHORTEST_PATHS => {
        //val score = shortestPathsMetric(groundTruth.SP, sample, landmarks, id)
        //score
        0.0
      }
      case MetricType.LABEL_PROPAGATION => {
        labelPropagationMetricOrig(groundTruth.LP, sample, 4, id)
      }
    }
  }
  */

  def pageRankEditMetricOrig(origRankGraph:Graph[Double, Double],
                     sampleGraph:Graph[Int, Int],
                     id: String, num: Int = 100, numIter: Int = 50, tol: Double = 0.001) : Int = {
    val sampleRankGraph = sampleGraph.pageRank(numIter)
    //val sampleRankGraph = sampleGraph.staticPageRank(numIter)
    val sortedOriginal = origRankGraph.vertices.sortBy(_._2, ascending=false).take(num).map(_._1)
    val sortedSample = sampleRankGraph.vertices.sortBy(_._2, ascending=false).take(num).map(_._1)
    val editDistance = Util.minEditDistance(sortedOriginal, sortedSample)
    val intersect = (Set() ++ sortedSample) intersect (Set() ++ sortedOriginal)
    val intersectDistance = intersect.size
    //sc.parallelize(sortedSample.toSeq).saveAsTextFile(s"${directory}/PR_edit/${id}")
    editDistance
  }

  def pageRankIntersectMetricOrig(origRankGraph:Graph[Double, Double],
                         sampleGraph:Graph[Int, Int],
                         id: String,  num: Int = 100, numIter: Int = 50, tol: Double = 0.001) : Int = {
    val sampleRankGraph = sampleGraph.pageRank(numIter)
    //val sampleRankGraph = sampleGraph.staticPageRank(numIter)
    val sortedOriginal = origRankGraph.vertices.sortBy(_._2, ascending = false).take(num).map(_._1)
    val sortedSample = sampleRankGraph.vertices.sortBy(_._2, ascending = false).take(num).map(_._1)
    val intersect = (Set() ++ sortedSample) intersect (Set() ++ sortedOriginal)
    val intersectDistance = intersect.size
    //sc.parallelize(sortedSample.toSeq).saveAsTextFile(s"${directory}/PR_intersect/${id}")
     intersectDistance
  }

  def connectedComponentsMetricOrig(origCCGraph: Graph[VertexId, Int],
                                sampleGraph: Graph[Int, Int],
                                id: String) : Double = {
    val sampleCCGraph = sampleGraph.connectedComponents()
    //sampleCCGraph.vertices.saveAsTextFile(s"${directory}/CC/${id}")
    return valueError(origCCGraph, sampleCCGraph)
  }

  def stronglyConnectedComponentsMetricOrig(origCCGraph: Graph[VertexId, Int],
                                        sampleGraph: Graph[Int, Int],
                                        numIter: Int,
                                        id: String) : Double = {
    val sampleCCGraph = sampleGraph.stronglyConnectedComponents(numIter)
    //sampleCCGraph.vertices.saveAsTextFile(s"${directory}/SCC/${id}")
    return valueError(origCCGraph, sampleCCGraph)
  }

  def triangleCountMetricOrig(origTriGraph: Graph[Int, Int],
                          sampleGraph: Graph[Int, Int],
                          id: String) : Double = {
    val sampleTriGraph = sampleGraph.triangleCount()
    //sampleTriGraph.vertices.saveAsTextFile(s"${directory}/TC/${id}")
    return triangleCountErrorAverage(origTriGraph, sampleTriGraph)
  }

  def shortestPathsMetricOrig(origSPGraph: Graph[ShortestPaths.SPMap, Int],
                          sampleGraph: Graph[Int, Int],
                          landmarks: Seq[VertexId],
                          id: String) : Double = {
    val sampleSPGraph = ShortestPaths.run(sampleGraph, landmarks)
    //sampleSPGraph.vertices.saveAsTextFile(s"${directory}/SP/${id}")
    return shortestPathsError(origSPGraph, sampleSPGraph, landmarks)
  }

  // The description of the label propagation algorithm can be found at:
  // https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/lib/LabelPropagation.scala
  def labelPropagationMetricOrig(origLabelGraph: Graph[VertexId, Int],
                             sampleGraph: Graph[Int, Int],
                             maxSteps: Int,
                             id: String) : Double = {
    val sampleLabelGraph = LabelPropagation.run(sampleGraph, maxSteps)
    //sampleLabelGraph.vertices.saveAsTextFile(s"${directory}/LP/${id}")
    return valueError(origLabelGraph, sampleLabelGraph)
  }

  def pageRankMetricWithAverageErrorOrig(sc:SparkContext,
                                     origRankGraph:Graph[Double, Double],
                                     sampleGraph:Graph[Int, Int],
                                     id: String,
                                     numIter: Int = 50,
                                     tol: Double = 0.001) : Double = {
    val sampleRankGraph = sampleGraph.pageRank(numIter)
    //sampleRankGraph.vertices.saveAsTextFile(s"${directory}/PR_avg/${id}")
    return pageRankErrorAverage(sc, origRankGraph, sampleRankGraph)
  }

  /*----------------------------------------------------------------------*/

  /*
  def run(metric: MetricType.MetricType,
          original: Any,
          sample: Graph[Int, Int],
          id:String = "",
          landmarks: Seq[VertexId] = Seq()): Double = {
    metric match{
      case MetricType.PAGERANK_EDIT => {
        pageRankEditMetricOld(original.asInstanceOf[Array[VertexId]], sample, id)
      }
      case MetricType.PAGERANK_INTERSECT => {
        pageRankIntersectMetricOld(original.asInstanceOf[Array[VertexId]], sample, id)
      }
      case MetricType.PAGERANK_AVERAGE_ERROR => {
        pageRankMetricWithAverageError(sc, original.asInstanceOf[Graph[Int, Int]], sample)
      }
      case MetricType.CC => {
        connectedComponentsMetric(original.asInstanceOf[Graph[Int, Int]], sample)
      }
      case MetricType.SCC => {
        stronglyConnectedComponentsMetric(original.asInstanceOf[Graph[Int, Int]], sample, 30)
      }
      case MetricType.TRIANGLE_COUNT => {
        triangleCountMetric(original.asInstanceOf[Graph[Int, Int]], sample)
      }
      case MetricType.SHORTEST_PATHS => {
        shortestPathsMetric(original.asInstanceOf[Graph[Int, Int]], sample, landmarks)
      }
      case MetricType.LABEL_PROPAGATION => {
        labelPropagationMetric(original.asInstanceOf[Graph[Int, Int]], sample, 10)
      }
    }
  }*/

  def pageRankMetric(graph: Graph[Int, Int],
                         sampleGraph:Graph[Int, Int],
                         id: String,
                         num: Int = 100,
                         numIter: Int = 50,
                         tol: Double = 0.001) : (Int, Int) = {
    val rankGraph = graph.pageRank(tol)
    val sampleRankGraph = sampleGraph.pageRank(tol)
    //val sampleRankGraph = sampleGraph.staticPageRank(numIter)
    val sortedOriginal = rankGraph.vertices.sortBy(_._2, ascending=false).take(num).map(_._1)
    val sortedSample = sampleRankGraph.vertices.sortBy(_._2, ascending=false).take(num).map(_._1)
    val editDistance = Util.minEditDistance(sortedOriginal, sortedSample)
    val intersect = (Set() ++ sortedSample) intersect (Set() ++ sortedOriginal)
    val intersectDistance = intersect.size
    //sc.parallelize(sortedSample.toSeq).saveAsTextFile(s"${directory}/PR_edit/${id}")
    (editDistance, intersectDistance)
  }

  def pageRankEditMetricOld(sortedOriginal:Array[VertexId],
                         sampleGraph:Graph[Int, Int],
                         id: String = "",
                         num: Int = 100,
                         numIter: Int = 50,
                         tol: Double = 0.001) : Int = {

    //val sampleRankGraph = sampleGraph.pageRank(tol)
    val sampleRankGraph = sampleGraph.staticPageRank(numIter)
    val sortedSample = sampleRankGraph.vertices.sortBy(_._2, ascending=false).take(num).map(_._1)
    val editDistance = Util.minEditDistance(sortedOriginal, sortedSample)
    editDistance
  }

  def pageRankIntersectMetricOld(sortedOriginal:Array[VertexId],
                              sampleGraph:Graph[Int, Int],
                              id: String,
                              num: Int = 100,
                              numIter: Int = 50,
                              tol: Double = 0.001) : Int = {

    //val sampleRankGraph = sampleGraph.pageRank(tol)
    val sampleRankGraph = sampleGraph.staticPageRank(numIter)
    val sortedSample = sampleRankGraph.vertices.sortBy(_._2, ascending=false).take(num).map(_._1)
    val intersect = (Set() ++ sortedSample) intersect (Set() ++ sortedOriginal)
    val intersectDistance = intersect.size
    intersectDistance
  }

  /*
  def connectedComponentsMetric(origGraph: Graph[Int, Int],
                                sampleGraph: Graph[Int, Int]) : Double = {
    val origCCGraph = origGraph.connectedComponents()
    val sampleCCGraph = sampleGraph.connectedComponents()
    return valueError(origCCGraph, sampleCCGraph)
  }
  */
  
  def stronglyConnectedComponentsMetric(origGraph: Graph[Int, Int],
                                        sampleGraph: Graph[Int, Int],
                                        numIter: Int) : Double = {
    val origCCGraph = origGraph.stronglyConnectedComponents(numIter)
    val sampleCCGraph = sampleGraph.stronglyConnectedComponents(numIter)
    return valueError(origCCGraph, sampleCCGraph)
  }

  
  def triangleCountMetric(origGraph: Graph[Int, Int],
                          sampleGraph: Graph[Int, Int]) : Double = {
    val origCCGraph = origGraph.triangleCount()
    val sampleCCGraph = sampleGraph.triangleCount()
    return triangleCountErrorAverage(origCCGraph, sampleCCGraph)
  }
  

  /*
  def shortestPathsMetric(origGraph: Graph[Int, Int],
                          sampleGraph: Graph[Int, Int],
                          landmarks: Seq[VertexId]) : Double = {
    val origSPGraph = ShortestPaths.run(origGraph, landmarks)
    val sampleSPGraph = ShortestPaths.run(sampleGraph, landmarks)
    return shortestPathsError(origSPGraph, sampleSPGraph, landmarks)
  }*/
  
  // The description of the label propagation algorithm can be found at:
  // https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/lib/LabelPropagation.scala
  def labelPropagationMetric(origGraph: Graph[Int, Int],
                             sampleGraph: Graph[Int, Int],
                             maxSteps: Int) : Double = {
    val origCCGraph = LabelPropagation.run(origGraph, maxSteps)
    val sampleCCGraph = LabelPropagation.run(sampleGraph, maxSteps)
    return valueError(origCCGraph, sampleCCGraph)
  }
  

  
  // Page rank version with average difference between the actual and sample 
  // page rank values as the evaluating metric.
  def pageRankMetricWithAverageError(sc:SparkContext,
                                     origGraph:Graph[Int, Int],
                                     sampleGraph:Graph[Int, Int],
                                     numIter: Int = 30,
                                     resetProb: Double = 0.001) : Double = {
    val origRankGraph = origGraph.pageRank(resetProb)
    val sampleRankGraph = sampleGraph.pageRank(resetProb)
    return pageRankErrorAverage(sc, origRankGraph, sampleRankGraph)
  }

}
