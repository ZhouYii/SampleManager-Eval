
import org.apache.spark.graphx._
import org.apache.spark.rdd.PairRDDFunctions

object KDegenrate {
    def run(graph: Graph[Int, Int], k: Int): Graph[Int, Int] = {
    val edgeMap = (new PairRDDFunctions(graph.degrees)).collectAsMap()
    //val edgeMap = (new PairRDDFunctions(graph.edges.map(e => (e.srcId, e.dstId)))).collectAsMap()
    val lpaGraph = graph.mapVertices{ case (vid, _) => if (edgeMap.getOrElse(vid, 0) <= k) 1 else 0}
    lpaGraph.vertices.foreach(e => println(e._1 + " : " + e._2))
    val outNeighbors = (new PairRDDFunctions(graph.outDegrees)).collectAsMap()
    
    def sendMessage(e: EdgeTriplet[Int, Int]) = {
      if (e.srcAttr.toInt == 1) {
        Iterator((e.dstId, e.srcAttr.toInt)) 
      } else {
        Iterator.empty
      }
    }
    
    def vertexProgram(vid: VertexId, attr: Int, message: Int) = {
      if (outNeighbors.getOrElse(vid, 0) + message <= k) {
        1
      } else {
        0
      }
    }
    
    val initialMessage = 0
    Pregel(lpaGraph, initialMessage, 15)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = (a, b) => a + b)
  }
}