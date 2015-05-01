import org.apache.spark.graphx._

import scala.collection.mutable.HashSet
import scala.util.Random

object SAType extends Enumeration {
  type SAType = Value
  val NODE, EDGE, EDGE_BY_VERTEX, RANDOM_JUMP, RANDOM_WALK, FOREST_FIRE, LAYERED, GUIDED = Value
}

abstract class SamplingAlgorithm{
  def sample(graph: Graph[Int, Int], fraction: Double) : Graph[Int, Int]
}

