import org.apache.spark.graphx._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.Random

// Implementation of Doubling Random Walk Generation from
// http://research.microsoft.com/apps/pubs/default.aspx?id=145447
object DoublingAlgorithm  {

  // Main doubling algorithm from the paper
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], lambda: Int, theta: Int)
  : RDD[(VertexId, ListBuffer[VertexId])] = {
    // Generate the random walk segments
    val segments = GenSeg.run[VD, ED](graph, lambda, theta).cache()
    var eta = math.ceil((lambda.toDouble)/theta).toInt
    val origEta: Int = eta
    // Generate the W and E structures from the paper as a single RD
    var we = genWE(segments, eta).cache()
    segments.unpersist(false)
    var oldWE: RDD[(VertexId, (Array[Array[ListBuffer[VertexId]]], Array[Array[VertexId]]))] = null
    while(eta > 1) {
      val etaP: Int = math.floor((eta+1.0)/2.0).toInt
      for (i <- 1 to etaP) {
        oldWE = we
        if(i.toDouble == (eta+1.0)/2.0){
          // copy the current random walk to the lower index
          we = copyRW(we, eta, etaP, i).cache()
        }else {
          // Join to get W[v,...] and E[v, ...]
          // where v = E[u, i, n] for all u in graph.vertices
          we = combineRW(we, eta, etaP, i).cache()
        }
        oldWE.unpersist(false)
        //WE.foreachPartition(x => {}) // materialize the structures
      }
      eta = etaP
    }
    // Map the starting vertex to the RW at the lowest index
    // Put the starting vertex at the front of the RW
    we.map(pair => (pair._1, ListBuffer(pair._1) ++ pair._2._1(1)(1)))
  }

  // Generate the W and E structures from the paper, combined into a single
  // RDD for efficient updating
  def genWE(s: RDD[(VertexId, Array[ListBuffer[VertexId]])], eta: Int)
  : RDD[(VertexId, (Array[Array[ListBuffer[VertexId]]], Array[Array[VertexId]]))] = {
    s.map(
      pair => {
        // W structure
        val w = new Array[Array[ListBuffer[VertexId]]](eta+1)
        for(i <- 1 to eta){
          w(i) = new Array[ListBuffer[VertexId]](eta+1)
          for(j <- 1 to eta) w(i)(j) = new ListBuffer[VertexId]()
          pair._2(i).trimStart(1) // remove this vertex from the start of the RW
          w(i)(eta) = pair._2(i)
        }
        // E structure
        val e = new Array[Array[VertexId]](eta+1)
        for(i <- 1 to eta){
          e(i) = new Array[VertexId](eta+1)
          e(i)(eta) = pair._2(i).last
        }
        (pair._1, (w, e))
      }
    )
  }

  // Copy the segments downward (from index eta to index etaP)
  def copyRW(we: RDD[(VertexId, (Array[Array[ListBuffer[VertexId]]], Array[Array[VertexId]]))],
               eta: Int, etaP: Int, i: Int)
  : RDD[(VertexId, (Array[Array[ListBuffer[VertexId]]], Array[Array[VertexId]]))] = {
    we.map(pair => {
      val w = pair._2._1
      val e = pair._2._2
      w(i)(etaP) = w(i)(eta)
      e(i)(etaP) = e(i)(eta)
      (pair._1, (w, e))
    })
  }

  // Combine segments to extend the RW (doubling the length of the current RW)
  def combineRW(we: RDD[(VertexId, (Array[Array[ListBuffer[VertexId]]], Array[Array[VertexId]]))],
                  eta: Int, etaP: Int, i: Int)
  : RDD[(VertexId, (Array[Array[ListBuffer[VertexId]]], Array[Array[VertexId]]))] = {
    // prepare for joining to the correct segments
    new PairRDDFunctions(we.map(pair => {
      val w = pair._2._1
      val e = pair._2._2
      (e(i)(eta), (w, e, pair._1))
    }))
      // Join using end of segment and start of segment
      // as respective keys
      .join(we)
      // Combine the segments
      .map(pair => {
      val w = pair._2._1._1
      val e = pair._2._1._2
      val w_ = pair._2._2._1
      val e_ = pair._2._2._2
      w(i)(etaP) = w(i)(eta) ++ w_(eta - i + 1)(eta)
      e(i)(etaP) = e_(eta - i + 1)(eta)
      (pair._2._1._3, (w, e))
    })
  }

}