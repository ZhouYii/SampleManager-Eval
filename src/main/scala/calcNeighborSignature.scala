import org.apache.spark.graphx._
import org.apache.spark.SparkContext

import scala.io.Source
import scala.collection.mutable.ListBuffer

import java.io.File

object Preprocessing {


  def signatures( graph: Graph[Int, Int]) : Array[(VertexId, Int)] = {
    val countGraph = graph.outerJoinVertices(graph.inDegrees) {
      case (id, attr, inDegOpt) => inDegOpt.getOrElse(0)
    }/*.outerJoinVertices(graph.outDegrees){
      case (id, count, outDegOpt) => count + outDegOpt.getOrElse(0)
    }*/
    countGraph.vertices.collect.sortBy(_._2)
  }

  /*
  def mapInsertIncrementCounter(map : scala.collection.mutable.Map[Long, Long], kvPair : (Long, Long)) : Unit =
  {
    if (map.contains(kvPair._1)) {
      map(kvPair._1) += 1
    } else {
      map += (kvPair._1 -> 1L )
    }
    Unit
  }

  def mapInsertToList(map : scala.collection.mutable.Map[Long, ListBuffer[Long]], kvPair : (Long, Long)) : Unit =
  {
    if (map.contains(kvPair._1)) {
      map(kvPair._1) += kvPair._2
    } else {
      map += (kvPair._1 -> ListBuffer(kvPair._2) )
    }
    Unit
  }



  def signatures2(data_file: String) : ListBuffer[(Long, Long)] = {

    var mapAccumulator: scala.collection.mutable.Map[Long, Long] =
                  scala.collection.mutable.Map()
    val addToAccumulator: ( (Long, Long) ) => Unit =
                  mapInsertIncrementCounter( mapAccumulator, _: (Long, Long) )
    var edgeList: ListBuffer[(Long, Long)] = ListBuffer()


    for (line <- Source.fromFile(data_file).getLines()) {
      val split = line.split('\t')
      edgeList += ((split(0).toLong, split(1).toLong))
    }

    edgeList.map(x => addToAccumulator(x))

    // Sort by neighborhood signature and print
    val list = mapAccumulator.toList sortBy ( _._2)
    list.to[ListBuffer]
  }
  */

}
