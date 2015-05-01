import org.apache.spark.graphx._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.Random

// Implementation of Doubling PPR from
// http://research.microsoft.com/apps/pubs/default.aspx?id=145447
object DoublingPersonalizedPageRank {

  // Generate a random number from a geometric distribution
  def geometricRandom(p: Double) : Int =
  {
    val u = Random.nextDouble()
    (math.log(u)/math.log(1 - p)).toInt + 1
  }

  // Get associated vertex counts from the generated random walk
  def getCountsFromRandomWalks(W: RDD[(VertexId, ListBuffer[VertexId])]) : RDD[((VertexId, VertexId), Int)] = {
    //W.collect.foreach(println)
    W.flatMap{case (v, rw) => {
      // construct map of counts from random walk
      val counts = mutable.HashMap[VertexId, Int]()
      for(v_ <- rw) counts.update(v_, counts.get(v_).getOrElse(0)+1)
      for(pair <- counts) yield ((v, pair._1), pair._2)
    }}
  }

  // Personalized Page Rank scores on the input graph
  // L is the expected per-vertex total random walk length
  // alpha is the random surfer teleport probability (back to initial vertex)
  // theta is a parameter specifying the segment length during generation
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], L: Int, alpha: Double, theta:Int =3)
  : RDD[((VertexId, VertexId), Double)] = {
    val R: Int = (alpha*L).toInt
    val d = DoublingAlgorithm
    var lambdaSum: Long = 0L
    var lambda = geometricRandom(alpha)
    lambdaSum += lambda
    var W = d.run[VD, ED](graph, lambda, theta).cache()
    var C = getCountsFromRandomWalks(W).cache()
    W.unpersist(false)
    var prevC: RDD[((VertexId, VertexId), Int)] = null
    for(i <- 2 to R){
      println("Iteration: " + i)
      var lambda = geometricRandom(alpha)
      lambdaSum += lambda
      // Generate the random walks using the Doubling algorithm
      W = d.run[VD, ED](graph, lambda, theta).cache()
      prevC = C
      // Merge the per-vertex random walk counts, updating existing counts
      // and adding new vertex pairs + counts
      C = new PairRDDFunctions(C)
        // merge counts
        .cogroup(getCountsFromRandomWalks(W))
        // aggregate counts
        .map(
          pair => (pair._1,
            (pair._2._1.fold(0)((a, b) => a+b) + pair._2._2.fold(0)((a, b) => a+b))
            )
        ).cache()
      W.unpersist(false)
      prevC.unpersist(false)
      C.foreachPartition(x => {}) // materialize the counts
      if(i % 50 == 0) {
        println("Checkpointing")
        C.checkpoint()
      }
    }
    new PairRDDFunctions(C).mapValues(_.toDouble/lambdaSum)
  }

}
