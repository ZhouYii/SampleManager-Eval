import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import scala.collection.mutable.{HashSet, ListBuffer}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.impl.EdgePartitionBuilder
import org.apache.spark.rdd.RDD

object LayeredSample {

  /*
   Given sorted array, max value and index range.
   Finds the index of smallest value <= max in arr. Returns -1 if none exists
   */
  def bstLEQ(arr:Array[(Edge[Int],HashSet[Edge[Int]])], 
             lBound:Int,
             rBound:Int,
             max:Int) : Int = 
   {
      if (arr(lBound)._2.size > max) {
        return -1
      }

      if (rBound - lBound <= 1) {
        return lBound
      }

      val median = (rBound + lBound) / 2
      if (arr(median)._2.size > max) {
        bstLEQ(arr, lBound, median - 1, max)
      } else {
        // Not using median + 1 to keep the invariant that median is an valid
        // index
        bstLEQ(arr, median, rBound, max) 
      }
   }

  def sum(a: Array[(VertexId, Int)], start:Int, end:Int) : Long = {
    var i = 0
    var sum = 0L
    for(i <- start to end){
       sum  = sum + a(i)._2
    }
    sum
  }

  def partitionBoundaryKMean(a: Array[(VertexId, Int)],
                             splitIdx: Int,
                              topSum: Long,
                              bottomSum: Long) : Double = {
    val n = a.size
    val topCentroid: Double = topSum/(n  - splitIdx - 1).toDouble
    val bottomCentroid: Double = bottomSum/(splitIdx+1).toDouble
    //val topCentroid: Double = sum(a, splitIdx+1, n-1) / (n  - splitIdx - 1).toDouble
    //val bottomCentroid: Double = sum(a, 0, splitIdx) / (splitIdx+1).toDouble
    //println(s"Top Centroid: ${topCentroid}, Bottom Centroid: ${bottomCentroid}")
    bottomCentroid + (topCentroid - bottomCentroid) / 2
  }

  def partitionByKMean(neighborSignatures: Array[(VertexId,Int)]) : Int = {
    /*
       Partitions the list of nodes by KMeans cluster with 2 centroids.
    */

    // Cutoff index is inclusive: [0,median], (median, n)
    var splitIdx = neighborSignatures.length / 2
    var bottomSum = sum(neighborSignatures, 0, splitIdx)
    var topSum = sum(neighborSignatures, splitIdx+1, neighborSignatures.size-1)
    var boundary = partitionBoundaryKMean(neighborSignatures, splitIdx, topSum, bottomSum)

    //println(s"Boundary1: ${boundary}")

    // Sliding window of observed centroid distances. Cycle's phase is 2 so we
    // use length-4 window to detect when two cycles have occurred. 1 cycle by
    // itself may be a fluke instead of convergence. Also, centroids cannot
    // overlap so 0.0 is a valid seed
    var cycle_detection = ListBuffer(0.0, 0.0, 0.0)
    while (cycle_detection(0) != cycle_detection(2) ||
      cycle_detection(1) != boundary) {

      //println(s"Boundary2: ${boundary}")

      // Update cycle window
      cycle_detection.remove(2)
      cycle_detection.insert(0, boundary)
      var signature = neighborSignatures(splitIdx+1)._2
      // Update partition based on boundary.
      while(signature < boundary){
          bottomSum = bottomSum + signature
          topSum = topSum - signature
          splitIdx = splitIdx + 1
          signature = neighborSignatures(splitIdx+1)._2
      }
      signature = neighborSignatures(splitIdx)._2
      while(signature > boundary){
        bottomSum = bottomSum - signature
        topSum = topSum + signature
        splitIdx = splitIdx - 1
        signature = neighborSignatures(splitIdx)._2
      }
      boundary = partitionBoundaryKMean(neighborSignatures, splitIdx, topSum, bottomSum)
    }
    splitIdx
  }

  /* TODO
  def sampleDenseEdges(denseEdges:HashSet[Edge[Int]], fraction:Double) = {
    if fraction >= 1.0 :
      return denseEdges

    val targetSize = (denseEdges.size * fraction).toInt
    val sampled:HashSet[Edge[Int]] = HashSet()
    while sampled.size < targetSize :

  }
  */

  def partitionEdges(graph: Graph[Int, Int],
                     sparseNodes:Array[(VertexId, Int)],
                     denseNodes:Array[(VertexId, Int)]) = {
    //println(s"Partitioning edgess")
    val sparseNodeSet = Map() ++ sparseNodes
    val denseNodeSet = Map() ++ denseNodes
    var sparseEdges: HashSet[Edge[Int]] = HashSet()
    var denseEdges: HashSet[Edge[Int]] = HashSet()
    var betweenEdges: HashSet[Edge[Int]] = HashSet()

    val allocateEdge = (e: Edge[Int]) => {
      val leftSparse = sparseNodeSet.contains(e.srcId)
      val rightSparse = sparseNodeSet.contains(e.dstId)

      if (leftSparse && rightSparse) {
        sparseEdges.add(e)

      } else if (!leftSparse && !rightSparse) {
        denseEdges.add(e)

      } else {
        betweenEdges.add(e)
      }
    }

    graph.edges.collect foreach allocateEdge
    (sparseEdges, denseEdges, betweenEdges)
  }

  def containingVertex(graph : Graph[Int,Int], vertexId: Long) = {
    graph.vertices.filter(v => v._1 == vertexId).count > 0
  }

  def VertexInArray(arr:Array[VertexId], target:VertexId) : Boolean = {
    VVertexInArray(arr, target)(0,arr.length-1)
  }

  // Search for vertexId in sorted Array of VertexId
  def VVertexInArray(arr:Array[VertexId], target:VertexId) 
                   (start: Int=0, end:Int=arr.length-1) : Boolean = {
    if (start > end) return false;
    val mid = start + (end-start+1)/2
    if (arr(mid) == target)
      return true
    else if (arr(mid) > target)
      return VVertexInArray(arr, target)(start, mid-1)
    else
      return VVertexInArray(arr, target)(mid+1, end)
  }

  // Greedy approx. to 0-1 knapsack problem
  // Assumes SampleSet is sorted by size of candidate set
  def FillSampleQuota(sampledVertex:HashSet[VertexId],
                      sampledEdge:HashSet[Edge[Int]],
                      targetVertices:Int,
                      sampleSet:Array[(Edge[Int],HashSet[Edge[Int]])])
  {
    val remainingVertex = targetVertices - sampledVertex.size
    var index = bstLEQ(sampleSet, 0, sampleSet.size, remainingVertex);

    // Halting conditions : 
    // 1) Sampled > targetVertices  
    // 2) We sampled the smallest possible candidate set
    while (
      sampledVertex.size < targetVertices &&
      index != -1)
    {
      val sampledEdges = sampleSet(index)._2 // Iterate over hashset of edges
      sampledEdges.foreach(edge => {
          sampledEdge += edge
          sampledVertex += edge.srcId
          sampledVertex += edge.dstId
        })

      if (index == 0) {
        index = -1
      } else {
        index = bstLEQ(sampleSet, 0, index, targetVertices-sampledVertex.size)
      }
    }
  }

  /*
   Performs sampling of sparse and dense nodes according to a split of 
   sparseFraction * targetNodes from sparse set and remaining from dense set
   */
  def GuidedClusterCoefInstance(sc:SparkContext,
                                graph:Graph[Int,Int],
                                sparseNodes:Array[VertexId],
                                denseNodes:Array[VertexId],
                                targetNodes:Int,
                                sparseFraction:Double) : Graph[Int,Int] = 
  {
      val sparseSampleSize = (sparseFraction * targetNodes).toInt

      /* Sample the sparse nodes and induce the sample's edges */
      val sparsePrior:Array[VertexId] =
              Helpers.sampleRandom(sparseNodes, sparseSampleSize).sorted
      val sparseEdges:Array[Edge[Int]] = graph.edges.filter(e => {
          (VertexInArray(sparsePrior, e.srcId) &&
           VertexInArray(sparsePrior, e.dstId))
        }).toArray

      var sampledVertex:HashSet[VertexId] = HashSet() ++ sparsePrior
      var sampledEdge:HashSet[Edge[Int]] = HashSet() ++ sparseEdges

      /* Use the edges reaching from sampled sparse nodes to dense nodes to
       * generate dense node candidates */
      val denseEdgeCandidates:Array[Edge[Int]] = graph.edges.filter(e => {
          (VertexInArray(sparsePrior, e.srcId) && 
           VertexInArray(denseNodes, e.dstId)) ||
          (VertexInArray(denseNodes, e.srcId) &&
           VertexInArray(sparsePrior, e.dstId))
          }).toArray
      val denseVertexCandidates:Array[VertexId] = denseEdgeCandidates.map(e => {
          if (VertexInArray(sparsePrior, e.srcId)) {
            e.dstId
          } else {
            e.srcId
          }
        }).toArray.sorted

      val linkingEdges:Array[Edge[Int]] = graph.edges.filter (e => {
          (VertexInArray(denseVertexCandidates, e.srcId) &&
            VertexInArray(denseVertexCandidates, e.dstId))
        }).toArray

      
      val pairs = linkingEdges.map(e => (e, HashSet(e)))
      val mapping : Map[Edge[Int],HashSet[Edge[Int]]] = pairs.toMap
      graph.edges.toArray.foreach(e => {
          if ((VertexInArray(sparsePrior, e.srcId) &&
                VertexInArray(denseVertexCandidates, e.dstId)) ||
              (VertexInArray(sparsePrior, e.dstId) &&
                VertexInArray(denseVertexCandidates, e.srcId))) 
          {
            val adjLinkingEdges = pairs.filter(adjEdge => 
              (adjEdge._1.srcId == e.srcId) ||
              (adjEdge._1.srcId == e.dstId) ||
              (adjEdge._1.dstId == e.srcId) ||
              (adjEdge._1.dstId == e.dstId)
            )
            adjLinkingEdges.foreach(x => mapping(x._1) += e)
          }
        })

      val samplePairs :List[(Edge[Int],HashSet[Edge[Int]])] = mapping.toList
      val sortedPairs = samplePairs.sortBy(x=>x._2.size)
      FillSampleQuota(sampledVertex, sampledEdge, targetNodes, pairs)
      //println(s"Sampled ${sampledVertex.size} of ${targetNodes}")
      val vertexRdd = sc.parallelize(sampledVertex
                                        .toSeq
                                        .map(x => (x,1)))
      val edgeRdd = sc.parallelize(sampledEdge.toSeq)
      //println(s"Dense nodes size ${denseNodes.size}")
      Graph(vertexRdd, edgeRdd)
    }

  def GuidedClustering(
                    sc:SparkContext, 
                    graph:Graph[Int,Int], 
                    fraction:Double) : Graph[Int,Int] =
  {
    var nodeArray: Array[(VertexId,Int)] = Preprocessing.signatures(graph)
    val splitIdx = partitionByKMean(nodeArray)
    val targetNodes = (graph.vertices.count() * fraction).toInt
    val (sparseNodes, denseNodes) = nodeArray.splitAt(splitIdx)
    var (sparseEdges, denseEdges, betweenEdges) = partitionEdges(graph, sparseNodes, denseNodes)
    val sparseNodes2 = sparseNodes.map(x => x._1)
    val denseNodes2 = denseNodes.map(x => x._1)
    val sampled = GuidedClusterCoefInstance(sc, graph, sparseNodes2, denseNodes2,
                                            targetNodes, 0.5)

    //println(s"Target Nodes: ${targetNodes} Sampled Nodes  ${sampled.vertices.count()} Sampled Edges ${sampled.edges.count()}")
    sampled
  }
  /*
  Get the layered sample. TODO - sample from "between"
  @param sc the Spark context
  @param data_file - the edge list file
  @param fraction - the fraction of nodes to sample
  @return the layered sample graph
   */
  def run(graph:Graph[Int, Int], fraction:Double): (Seq[(VertexId, Int)], Seq[Edge[Int]]) = {
    //val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, data_file)
    //graph.cache()
    //println(s"Calculating signatures")
    var nodeArray: Array[(VertexId,Int)] = Preprocessing.signatures(graph)
    //println(s"Partitioning Vertices")
    val splitIdx = partitionByKMean(nodeArray)
    //println(s"Splitting")
    val (sparseNodes, denseNodes) = nodeArray.splitAt(splitIdx)
    //println("Partitioning Edges")
    var (sparseEdges, denseEdges, betweenEdges) = partitionEdges(graph, sparseNodes, denseNodes)
    //println(s"Sampling from sparse")
    val (sampleSparseVertices, sampleSparseEdges) = Util.randomJumpSample(sparseNodes.sorted, sparseEdges.toArray, fraction)
    // val (sampleDense, denseVertex) = Sampling.randomJumpSample(sc, dense_nodes.sorted, dense.toArray, 1.0)
    // val sampleDense = Graph(sc.parallelize(dense_nodes.toSeq), sc.parallelize(dense.toSeq))
    //println(s"Building send map")
    val denseVertices = Map() ++ denseNodes

   //println(s"Filtering between edges")
    // Filter by new sparse/dense edges.
    val betweenSize = scala.math.pow((sampleSparseVertices.size + denseVertices.size), 1.3).toInt

    var inducedBetweenEdges = betweenEdges.filter(e =>
      (denseVertices.contains(e.srcId) && sampleSparseVertices.contains(e.dstId)) ||
      (denseVertices.contains(e.dstId) && sampleSparseVertices.contains(e.srcId))).slice(0, betweenSize)

    //println(s"Calculating between size")
    val edges:HashSet[Edge[Int]] = sampleSparseEdges.union(inducedBetweenEdges).union(denseEdges)
    val vertices:HashSet[(VertexId, Int)] = HashSet() ++ denseVertices ++ sampleSparseVertices
    (vertices.toSeq, edges.toSeq)
  }

  /*
  def buildGraph(sc: SparkContext, edges:Seq[Edge[Int]]) : Graph[Int, Int] = {
    val edgesP = sc.parallelize(edges).mapPartitionsWithIndex{ (pid, iter) =>
      val builder = new EdgePartitionBuilder[Int, Int]
      iter.foreach{e => builder.add(e.srcId, e.dstId, 1)
      }
      Iterator((pid, builder.toEdgePartition))
    }.persist(StorageLevel.MEMORY_ONLY)
    edgesP.count()

    GraphImpl.fromEdgePartitions(edgesP, defaultVertexAttr = 1)
  }
  */

  def partitionBoundaryKMean2(top: ListBuffer[(Long,Long)],
                          bottom: ListBuffer[(Long,Long)]) : Double = {

    def sum(l: ListBuffer[(Long,Long)]) = l.foldLeft(0L)((r,c) => r+c._2)
    val topCentroid: Double = sum(top) / top.length.toDouble
    val bottomCentroid: Double = sum(bottom) / bottom.length.toDouble

    //println(s"Top Centroid: ${topCentroid}, Bottom Centroid: ${bottomCentroid}")

    bottomCentroid + (topCentroid - bottomCentroid) / 2
  }

/*
  def partitionEdges2(edgeFile: String,
                     sparseNodes:ListBuffer[Long],
                     denseNodes:ListBuffer[Long]) = {
    println(s"Partitioning edges")


    var sparseEdges: ListBuffer[Edge[Int]] = ListBuffer()
    var denseEdges: ListBuffer[Edge[Int]] = ListBuffer()
    var betweenEdges: ListBuffer[Edge[Int]] = ListBuffer()

    val edges = Source.fromFile(edgeFile).getLines().map(l => {
        val pair = l.split('\t')
        Edge(pair(0).toLong, pair(1).toLong, 1)
      })
    println(s"Edges read")
    //var count = 0

    val sparseNodeSet = Set() ++ sparseNodes
    val denseNodeSet = Set() ++ denseNodes

    val allocateEdge = (e: Edge[Int]) => {
      val leftSparse = sparseNodeSet.contains(e.srcId)//sparseNodes.contains(e.srcId)
      val rightSparse = sparseNodeSet.contains(e.dstId)//sparseNodes.contains(e.dstId)
      //println(s"Checked sparse")
      //count += 1
      //println(s"${count}")
      if (leftSparse && rightSparse) {
        sparseEdges.append(e)
      } else if (!leftSparse && !rightSparse) {
        denseEdges.append(e)
      } else {
        betweenEdges.append(e)
      }
    }

    edges foreach allocateEdge
    println(s"Between edge size: ${betweenEdges.size}")
    println(s"Dense edge size: ${denseEdges.size}")
    println(s"Sparse edge size: ${sparseEdges.size}")
    (sparseEdges, denseEdges, betweenEdges)
  }
  */

  def partitionByKMean2(neighborSignatures: ListBuffer[(Long,Long)]) = {
    /*
       Partitions the list of nodes by KMeans cluster with 2 centroids.
    */

    // Cutoff index is inclusive: [0,median], (median, n)
    val median = neighborSignatures.length / 2
    var (bottom, top) = neighborSignatures.splitAt(median)
    var boundary = partitionBoundaryKMean2(top, bottom)

    //println(s"Boundary: ${boundary}")


    // Sliding window of observed centroid distances. Cycle's phase is 2 so we
    // use length-4 window to detect when two cycles have occurred. 1 cycle by
    // itself may be a fluke instead of convergence. Also, centroids cannot
    // overlap so 0.0 is a valid seed
    var cycle_detection = ListBuffer(0.0, 0.0, 0.0)
    while (cycle_detection(0) != cycle_detection(2) ||
           cycle_detection(1) != boundary) {

      //println(s"Boundary: ${boundary}")

      // Update cycle window
      cycle_detection.remove(2)
      cycle_detection.insert(0, boundary)

      // Update partition based on boundary.
      while (top.head._2 < boundary) {
        bottom.append(top.remove(0))
      }
      while (bottom.last._2 > boundary) {
        top.insert(0, bottom.remove(bottom.length - 1))
      }

      boundary = partitionBoundaryKMean2(top, bottom)
    }

    (bottom, top)
  }

  /*
  def loadNeighborhoodSignature(tsv_file:String) {
    var nodeList: ListBuffer[(Long,Long)] = ListBuffer()
    // Read in list of (nodeID, #_incident_nodes), sorted by incident nodes
    for (line <- Source.fromFile(tsv_file).getLines()) {
      val split = line.split('\t')
      nodeList += ((split(0).toLong, split(1).toLong))
    }
    nodeList
  }*/

}
