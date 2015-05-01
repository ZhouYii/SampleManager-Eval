import org.apache.spark.graphx._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.Random

object GenSeg {


  // Generate random walk segments for all vertices in the input graph
  // lambda specifies the total random walk length,
  // theta specifies the segment sizes
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], lambda: Int, theta: Int): RDD[(VertexId, Array[ListBuffer[VertexId]])] = {
    val eta: Int = math.ceil((lambda.toDouble)/theta).toInt
    val phi: Int = math.floor((lambda.toDouble)/theta).toInt
    var S = graph.vertices.map(
      v => (v._1, new Array[ListBuffer[VertexId]](eta+1).map(
        a => new ListBuffer[VertexId] += v._1
      ))
    ).cache()
    val neighbors = graph.collectNeighborIds(EdgeDirection.Either).cache()
    var oldS: RDD[(VertexId, Array[ListBuffer[VertexId]])] = null
    for(j <- 1 to theta){
    //(1 to theta).foreach(j=>{
      var done = false
      var i: Int = 1
      while(i <= eta && !done) {
        val i_ = i
        val num = lambda - theta * phi.toInt
        if ((i == eta) && (num > 0 && num < j)) {
          done = true
        } else {
          val Sp = new PairRDDFunctions(S.map(
            v => (v._2(i_).last, v._2))
          ).join(neighbors).cache()
          oldS = S
          S = Sp.map(
            U => {
              val nArr = U._2._2
              val vd = U._2._1
              vd(i_).append(nArr(Random.nextInt(nArr.size)))
              (vd(i_).head, vd)
            }).cache()
          Sp.unpersist(false)
          oldS.unpersist(false)
          //S.foreachPartition(x => {}) // materialize the segments
        }
        i += 1
      }
      //})
      /*
      var done = false
      for(i <- 1 to eta) {
        if (!done) {
          val num = lambda - theta * phi.toInt
          if ((i == eta)
            && (num > 0 && num < j)) {
            println("here")
            done = true
          }else {
            val Sp = new PairRDDFunctions(S.map(
              v => (v._2(i).last, v._2))
            ).join(neighbors).cache()
            oldS = S
            S = Sp.map(
              U => {
                val nArr = U._2._2
                val vd = U._2._1
                vd(i).append(nArr(Random.nextInt(nArr.size)))
                (vd(i).head, vd)
              }).cache()
            Sp.unpersist(false)
            oldS.unpersist(false)
            //S.foreachPartition(x => {}) // materialize the segments
          }
        }
      }
      */
    }
    S
  }

  /*
  def genSeg(graph: Graph[Int, Int], lambda: Int, theta: Int)
  : RDD[(VertexId, Array[ListBuffer[VertexId]])] = {
    val eta: Int = math.ceil((1.0*lambda)/theta).toInt
    val phi: Int = math.floor((1.0*lambda)/theta).toInt
    var S = graph.vertices.map(v =>
      (v._1, new Array[ListBuffer[VertexId]](eta+1).map(
        a => new ListBuffer[VertexId] += v._1
      ))
    )
    val neighbors = graph.collectNeighborIds(EdgeDirection.Either)
    neighbors.cache()
    for(j <- 1 to theta){
      var done = false
      for(i <- 1 to eta) {
        if (!done) {
          var num = lambda - theta * phi.toInt
          if ((i == eta)
            && (num > 0 && num < j)) {
            done = true
          }else {
            S = S.map(v => (v._2(i).last, v._2))
            val Sp = new PairRDDFunctions(S).join(neighbors)
            Sp.cache()
            S = Sp.map(U => {
              val nArr = U._2._2
              val vd = U._2._1
              vd(i).append(nArr(Random.nextInt(nArr.size)))
              (vd(i).head, vd)
            })
            Sp.unpersist(false)
          }
        }
      }
    }
    S
  }
  */
}
