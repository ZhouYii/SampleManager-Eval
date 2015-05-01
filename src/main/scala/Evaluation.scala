import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.GraphOps
import scala.collection.mutable.ListBuffer

import SAType._
import MetricType._

import scala.util.Random

case class EvaluationResult(id: String, metric: String, score: Double) extends Serializable {
  override
  def toString(): String = {
    s"${id} ${metric} ${score}"
  }
}
case class SampleResult(id: String, vertices: Double, edges: Double) extends Serializable {
  override
  def toString(): String = {
    s"${id} ${vertices} ${edges}"
  }
}

object Evaluation {

  def runNew(sc: SparkContext, edgeFile: String, epsilon:Double=0.001, numIter:Int=30): Unit = {
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, edgeFile)
    graph.cache()
    val directory = s"data/evaluation/${Util.getDate()}"
    println("Building ground truth")
    val groundTruth = new AlgorithmResults(sc, directory, graph)
    println("Ground truth complete")
    //groundTruth.store()
    graph.unpersist(false)
    println("Ground truth stored")

    val fracs = List(0.1, 0.2, 0.3, 0.4, 0.5)
    val metrics = List(MetricType.PAGERANK_EDIT, MetricType.PAGERANK_INTERSECT, MetricType.PAGERANK_AVERAGE_ERROR,
      MetricType.CC, MetricType.SCC,  MetricType.LABEL_PROPAGATION, MetricType.SHORTEST_PATHS)
    val evalResults = new ListBuffer[EvaluationResult]()
    val sampleResults = new ListBuffer[SampleResult]()
    val landmarks = graph.vertices.takeSample(false, 100, Random.nextLong).map(_._1).toSeq
    println("Landmarks constructed")
    //val (sparse, dense) = Util.partitionByKMeans(graph.degrees)

    val m = new Metric(sc, directory)
    // run the metrics on a sample graph
    def runMetricsAndSave(sample: Graph[Int, Int], fraction: Double, id: String): Unit = {
      val sampleAlgorithmResults = new AlgorithmResults(sc, directory, sample, id, landmarks = landmarks)
      //sampleAlgorithmResults.store()
      for(metric: MetricType.MetricType <- metrics){//MetricType.values) {
        println(s"Running metric ${metric.toString}")
        var score = 0.0
        if(metric == MetricType.SHORTEST_PATHS) {
         score = m.runR(metric, groundTruth, sampleAlgorithmResults, landmarks)
        }else{
          score = m.runR(metric, groundTruth, sampleAlgorithmResults)
        }
        evalResults.append(new EvaluationResult(id, metric.toString, score))
      }
    }

    // run the evaluation
    def runEval(alg: SAType.Value, fraction: Double): Unit ={
      val id = s"${alg.toString}/${fraction}"
      println(s"Running evaluation for ${id} ${alg.toString}")
      var sample: Graph[Int, Int] = null
      alg match {
        case SAType.EDGE => {
          sample = EdgeSampling().sample(graph, fraction)
        }
        case SAType.EDGE_BY_VERTEX => {
          sample = EdgeByVertexSampling().sample(graph, fraction)
        }
        case SAType.NODE => {
          sample = NodeSampling().sample(graph, fraction)
        }
        case SAType.FOREST_FIRE => {
          sample = ForestFireInducedSampling(sc).sample(graph, fraction)
        }
        case SAType.LAYERED => {
          //sample = WeightedLayeredSampling(fraction, sparse, dense).unweightedSample2(graph)
          //sample = LayeredSample(fraction).sample(graph)
          val (vertices, edges) = LayeredSample.run(graph, fraction)
          sample = Graph(sc.parallelize(vertices), sc.parallelize(edges))
        }
        case SAType.GUIDED => {
          sample = LayeredSample.GuidedClustering(sc, graph, fraction)
        }
        case SAType.RANDOM_JUMP => {
          sample = RandomJumpSampling(sc).sampleEdges(graph, fraction)
        }
        case SAType.RANDOM_WALK => {
          sample = RandomWalkSampling(sc).sampleEdges(graph, fraction)
        }
      }
      sample.cache()
      sampleResults.append(new SampleResult(id, sample.vertices.count, sample.edges.count))
      runMetricsAndSave(sample, fraction, id)
      sample.unpersist(false)
      println(s"Completed evaluation for ${id}")
    }
    println("Beginning evaluation")
    for(f_ <- fracs) {
      runEval(SAType.EDGE, f_)
      runEval(SAType.EDGE_BY_VERTEX, f_)
      runEval(SAType.NODE, f_)
      runEval(SAType.FOREST_FIRE, f_)
      runEval(SAType.LAYERED, f_)
      runEval(SAType.GUIDED, f_)
      runEval(SAType.RANDOM_JUMP, f_)
      runEval(SAType.RANDOM_WALK, f_)
    }
    sc.parallelize(sampleResults.toSeq).saveAsTextFile(s"${directory}/sample_results")
    sc.parallelize(evalResults.toSeq).saveAsTextFile(s"${directory}/eval_results")
  }

  /*
   Return a list of tuples : (componentID, # vertices)
   */
  def countComponents(g:Graph[Int,Int]) = {
    val graphOps = new GraphOps(g)
    val countGraph = graphOps.connectedComponents()
    val componentIdSequence = countGraph.vertices.map(v => v._2)
    val components = componentIdSequence.groupBy(l => l)
    // pair._2._1 references id of the component, and pair._2.size is the freq
    val componentMultiplicity = components.map(pair => (pair._1, pair._2.size))
    componentMultiplicity sortBy (_._2)
  }



  def run(sc: SparkContext, edgeFile: String, epsilon:Double=0.001, numIter:Int=30) : Unit = {

    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, edgeFile)
    graph.cache()

    println("Running pageRank on original graph")
    val origRankGraph = graph.pageRank(epsilon)//.staticPageRank(30)
    println("Original graph pageRank done")
    graph.unpersist(false)
    origRankGraph.cache()
    val sortedOriginal = origRankGraph.vertices.sortBy(_._2, ascending=false).take(100).map(_._1)
    origRankGraph.unpersist(false)

    val directory = s"data/evaluation/${Util.getDate()}"

    sc.parallelize(sortedOriginal.toSeq).saveAsTextFile(s"${directory}/sorted_original")

    val fracs = List(0.1, 0.2, 0.3, 0.4, 0.5)//, 0.6, 0.7, 0.8, 0.9)

    val evalResults = new ListBuffer[EvaluationResult]()
    val sampleResults = new ListBuffer[SampleResult]()


    val m = new Metric(sc, directory)

    def runMetricsAndSave(sample: Graph[Int, Int], fraction: Double, id: String): Unit = {
      for(metric: MetricType.MetricType <- List(MetricType.PAGERANK_EDIT)){//MetricType.values) {
        println(s"Running metric ${metric.toString}")
        val score = m.run(metric, sortedOriginal, sample, id)
        evalResults.append(new EvaluationResult(id, metric.toString+"_edit", score))
        //evalResults.append(new EvaluationResult(id, metric.toString+"_intersect", score))
      }
    }

    def runEval(alg: SAType.Value, fraction: Double): Unit ={
      val id = s"${alg.toString}/${fraction}"
      println(s"Running evaluation for ${id}")
      var sample: Graph[Int, Int] = null
      alg match {
        case SAType.EDGE => {
          sample = EdgeSampling().sample(graph, fraction)
        }
        case SAType.EDGE_BY_VERTEX => {
          sample = EdgeByVertexSampling().sample(graph, fraction)
        }
        case SAType.NODE => {
          sample = NodeSampling().sample(graph, fraction)
        }
        case SAType.FOREST_FIRE => {
          //sample = Sampling.forestFireSamplingInduced(sc, graph, fraction)
          //sample = ForestFireInducedSampling(sc, fraction).sample(graph)
          return
        }
        case SAType.LAYERED => {
          val (vertices, edges) = LayeredSample.run(graph, fraction)
          sample = Graph(sc.parallelize(vertices), sc.parallelize(edges))
        }
        case SAType.GUIDED => {
          sample = LayeredSample.GuidedClustering(sc, graph, fraction)
        }
        case SAType.RANDOM_JUMP => {
          //sample = Sampling.randomJumpSampleGraph(sc, graph.vertices.collect.sorted, graph.edges.collect, fraction)
          sample = RandomJumpSampling(sc).sampleEdges(graph, fraction)
        }
        case SAType.RANDOM_WALK => {
          //sample = Sampling.randomWalkSampleGraph(sc, graph.vertices.collect.sorted, graph.edges.collect, fraction)
          sample = RandomWalkSampling(sc).sampleEdges(graph, fraction)
        }
      }
      sample.cache()
      sampleResults.append(new SampleResult(id, sample.vertices.count, sample.edges.count))
      runMetricsAndSave(sample, fraction, id)
      sample.unpersist(false)
      println(s"Completed evaluation for ${id}")
    }

    for(f_ <- fracs) {
      runEval(SAType.EDGE, f_)
      runEval(SAType.EDGE_BY_VERTEX, f_)
      runEval(SAType.NODE, f_)
      //runEval(SAType.FOREST_FIRE, f_)
      runEval(SAType.LAYERED, f_)
      runEval(SAType.GUIDED, f_)
      runEval(SAType.RANDOM_JUMP, f_)
      runEval(SAType.RANDOM_WALK, f_)
    }
    sc.parallelize(sampleResults.toSeq).saveAsTextFile(s"${directory}/sample_results")
    sc.parallelize(evalResults.toSeq).saveAsTextFile(s"${directory}/eval_results")
  }


}
