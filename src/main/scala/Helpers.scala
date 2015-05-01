import scala.util.Random

import scala.collection.mutable.HashSet

object Helpers {

  // Assumes numToSample <= arr.size
  def sampleRandom(arr:Array[Long], numToSample:Int) : Array[Long]= {
    val randIndices:HashSet[Int] = HashSet()
    while (randIndices.size < numToSample)
      randIndices += Random.nextInt(arr.size)
    val indexArr:Array[Int] = randIndices.toArray
    indexArr.map(x => arr(x))
  }

}
