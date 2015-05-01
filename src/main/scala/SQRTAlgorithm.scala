import org.apache.spark.SparkException
import org.apache.spark.graphx._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.mllib.linalg
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer

// Implementation of SQRT Random Walk algorithm from
// http://research.microsoft.com/apps/pubs/default.aspx?id=145447
object SQRTAlgorithm {

  // Main SQRT algorithm from the paper
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], lambda: Int, theta: Int) : RDD[(VertexId, ListBuffer[VertexId])] = {
    val S = GenSeg.run[VD, ED](graph, lambda, theta).cache()
    var W = graph.vertices.map(v => (v._1, (new ListBuffer())+=v._1)).cache()
    var oldW: RDD[(VertexId, ListBuffer[VertexId])] = null
    for(i <- 1 to math.ceil((lambda*1.0)/theta).toInt){
      oldW = W
      // prepare for joining
      W = new PairRDDFunctions(W.map(v => (v._2.last, v._2)))
        // join with the appropriate segments
        .join(S)
        // add the corresponding segment to the RW
        .map(
          U => {
            val sArr = U._2._2
            val wList = U._2._1
            wList.remove(wList.size-1)
            wList ++= sArr(i)
            (wList.head, wList)
          }).cache()
      oldW.unpersist(false)
    }
    //W.foreach(u => u._2.remove(0))
    W
  }
}
