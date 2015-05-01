import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.collection.mutable.ListBuffer

object Timing {

  def run(sc: SparkContext, graph: Graph[Int, Int]) : Unit = {

    val fracs = new ListBuffer[Double]()
    var frac: Double = 0.05
    while(frac < 1.0){
      fracs.append(frac)
      frac += 0.05
    }
    // to warm up the cache
    for(i<-0 to fracs.size-1){
      val s = EdgeSampling().sample(graph, fracs(i))
      s.cache()
      s.edges.foreachPartition(x => {})
      s.unpersist(false)
    }

    val results = new ListBuffer[(String, Double, Double)]()
    println("Node Sampling:")
    var t = timeSampling(NodeSampling(), graph)
    for(t_ <- t){
      results.append(("NODE", t_._1, t_._2))
    }
    println("-----------------------------")
    println("Edge by Vertex Sampling:")
    t = timeSampling(EdgeByVertexSampling(), graph)
    for(t_ <- t){
      results.append(("EDGE_BY_VERTEX", t_._1, t_._2))
    }
    println("-----------------------------")

    println("Edge Sampling:")
    t = timeSampling(EdgeSampling(), graph)
    for(t_ <- t){
      results.append(("EDGE", t_._1, t_._2))
    }
    println("-----------------------------")

    println("RJ Sampling:")
    t = timeSampling(RandomJumpSampling(sc), graph)
    for(t_ <- t){
      results.append(("RJ", t_._1, t_._2))
    }
    println("-----------------------------")
    println("RW Sampling:")
    t = timeSampling(RandomWalkSampling(sc), graph)
    for(t_ <- t){
      results.append(("RW", t_._1, t_._2))
    }
    println("-----------------------------")
    println("Forest Fire Induced Sampling:")
    t = timeSampling(ForestFireInducedSampling(sc), graph)
    for(t_ <- t){
      results.append(("FF", t_._1, t_._2))
    }
    sc.parallelize(results).saveAsTextFile("data/results.txt")
  }

  def timeSampling(sa: SamplingAlgorithm, graph: Graph[Int, Int]) : ListBuffer[(Double, Double)] = {

    val results = new ListBuffer[(Double, Double)]()
    val fracs = new ListBuffer[Double]()
    var frac: Double = 0.05
    while(frac < 1.0){
      fracs.append(frac)
      frac += 0.05
    }

    for(i<-0 to fracs.size-1){
      graph.cache()
      var time = 0.0
      for(j <- 1 to 10) {
        val now = System.nanoTime
        val s = sa.sample(graph, fracs(i))
        //s.cache()
        s.edges.foreachPartition(x => {})
        val elapsed = System.nanoTime - now
        s.unpersist(false)
        time += elapsed/1000000000.0
      }
      fracs(i) = BigDecimal(fracs(i)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      results.append((fracs(i), time/25.0))
      println(fracs(i) + " " + time/25.0)
    }
    results
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000.0 + "ns")
    result
  }

}
