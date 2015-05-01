//import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.graphx._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable
import scala.util.Random


case class WeightedLayeredSampling(sparse: VertexRDD[Int], dense: VertexRDD[Int]) extends SamplingAlgorithm{



  implicit val degreeOrdering = new Ordering[(VertexId, Int)] {
    override def compare(a: (VertexId, Int), b: (VertexId, Int)) = a._2.compare(b._2)
  }

  def partitionByKMeans(graph: Graph[Int, Int]) : (VertexRDD[Int], VertexRDD[Int]) = {
    Util.partitionByKMeans(graph.degrees)
  }



  // collect the maximum neighbor degree for each vertex
  def collectNeighborMaxDegree[ED](graph: Graph[Int, ED], edgeDirection: EdgeDirection): VertexRDD[Int] = {
    val nbrs = edgeDirection match {
      case EdgeDirection.Either =>
        graph.aggregateMessages[Int](
          ctx => {
            ctx.sendToSrc(ctx.dstAttr)
            ctx.sendToDst(ctx.srcAttr)
          },
          (a, b) => Math.max(a, b), TripletFields.All)
      case EdgeDirection.In =>
        graph.aggregateMessages[Int](
          ctx => ctx.sendToDst(ctx.srcAttr),
          (a, b) => Math.max(a, b), TripletFields.Src)
      case EdgeDirection.Out =>
        graph.aggregateMessages[Int](
          ctx => ctx.sendToSrc(ctx.dstAttr),
          (a, b) => Math.max(a, b), TripletFields.Dst)
      case EdgeDirection.Both =>
        throw new SparkException("collectEdges does not support EdgeDirection.Both. Use" +
          "EdgeDirection.Either instead.")
    }
    sparse.leftJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(0)
    }
  } // end of collectNeighbor

  def sample(graph:Graph[Int, Int], fraction: Double): Graph[Int, Int] = {
    val degreeGraph = graph.outerJoinVertices(graph.degrees) {
      case (id, u, degOpt) =>degOpt.getOrElse(0)
    }
    val sparseNeighborMaxDegrees = collectNeighborMaxDegree(degreeGraph, EdgeDirection.Either)
    val maxSparseDegree:Double = sparseNeighborMaxDegrees.max()(degreeOrdering)._2.toDouble
    println(maxSparseDegree)
    var sampleVertices = sparseNeighborMaxDegrees.filter(v => {
      val rnd = Random.nextDouble()
      if(rnd < v._2.toDouble/maxSparseDegree){
        true
      }else {
        false
      }
    })
    val groupedSrcEdges:RDD[(VertexId, Iterable[Edge[Int]])] = graph.edges.groupBy(e => e.srcId)
    val sampleSrcGroup = sampleVertices.leftJoin(groupedSrcEdges){
      (v, vd, u) => u.getOrElse(Iterable())
    }
    val sampleSrcEdges:RDD[Edge[Int]] = sampleSrcGroup.flatMap{
      case (v, n) => n
    }
    val retainedDense = sampleSrcEdges.map(_.dstId).intersection(dense.map(_._1))
    println(dense.count)
    println(retainedDense.count)
    sampleVertices = VertexRDD(sampleVertices.union(retainedDense.map((_, 0))))
    val srcGroup = sampleVertices.leftJoin(groupedSrcEdges){
      (v, vd, u) => u.getOrElse(Iterable())
    }
    val srcEdges:RDD[Edge[Int]] = srcGroup.flatMap{
      case (v, n) => n
    }
    println(s"Edge count: ${srcEdges.count}")
    val groupedDstEdges = srcEdges.groupBy(e => e.dstId)
    val dstGroup = sampleVertices.leftJoin(groupedDstEdges){
      (v, vd, u) => u.getOrElse(Iterable())
    }
    val sampleEdges: RDD[Edge[Int]] = dstGroup.flatMap{
      case (v, n) => n
    }
    println(s"Sample edge count: ${sampleEdges.count}")
  ///////
    /*
    val graphNeighbors = graph.collectNeighbors(EdgeDirection.Either)
    val sparseSampleNeighbors = sparseSample.leftJoin(graphNeighbors){
      (v, vd, u) => u.getOrElse(Array())
    }

    val edges:RDD[Edge[Int]] = sparseSampleNeighbors.flatMap{
      case (v, n) => {
        for(n_ <- n) yield Edge[Int](v, n_._1, 1)
      }
    }
    */
    Graph.fromEdges(sampleEdges, defaultValue = 1)
  }


  // our previous layered sampling algorithm implemented within Spark
  def unweightedSample(graph:Graph[Int, Int], fraction: Double): Graph[Int, Int] = {
    // retain all dense vertices, take random sample of sparse vertices
    var sampleVertices:VertexRDD[Int] = VertexRDD(sparse.sample(false, fraction, Random.nextLong).union(dense))
    // reduce to only edges with srcId \in sampleVertices
    val groupedSrcEdges:RDD[(VertexId, Iterable[Edge[Int]])] = graph.edges.groupBy(e => e.srcId)
    val srcGroup = sampleVertices.leftJoin(groupedSrcEdges){
      (v, vd, u) => u.getOrElse(Iterable())
    }
    val srcEdges:RDD[Edge[Int]] = srcGroup.flatMap{
      case (v, n) => n
    }
    // reduce to only edges with dstId \in sampleVertices
    val groupedDstEdges = srcEdges.groupBy(e => e.dstId)
    val dstGroup = sampleVertices.leftJoin(groupedDstEdges){
      (v, vd, u) => u.getOrElse(Iterable())
    }
    val sampleEdges: RDD[Edge[Int]] = dstGroup.flatMap{
      case (v, n) => n
    }
    // build the graph from the edges
    val g = Graph.fromEdges(sampleEdges, defaultValue = 1)
    println(g.vertices.count)
    g
  }

  // our previous layered sampling algorithm implemented within Spark
  def unweightedSample2(graph:Graph[Int, Int], fraction: Double): Graph[Int, Int] = {
    // retain all dense vertices, take random sample of sparse vertices
    var sampleVertices = sparse.sample(false, fraction, Random.nextLong).union(dense)
    val pair = new PairRDDFunctions(sampleVertices)
    val edges1: RDD[(VertexId, VertexId)] = graph.edges.map(e => (e.srcId, e.dstId))
    edges1.repartition(sampleVertices.partitions.size)
    val ret1 = pair.join(edges1).map(v => (v._1, v._2._2))
    val edges2: RDD[(VertexId, VertexId)] = ret1.map(e => (e._2, e._1))
    val ret2 = pair.join(edges2).map(v => (v._2._2, v._1))
    var intersection: RDD[Edge[Int]] = ret1.intersection(ret2).map(v => Edge(v._1, v._2, 0))
    intersection.repartition(graph.edges.partitions.size)
    println(intersection.count)
    Graph.fromEdges(intersection, defaultValue = 1)
  }


  /*
  // our previous layered sampling algorithm implemented within Spark
  def unweightedSample3(graph:Graph[Int, Int], fraction: Double): Graph[Int, Int] = {
    // retain all dense vertices, take random sample of sparse vertices
    var sampleVertices = sparse.sample(false, fraction, Random.nextLong).union(dense)
    val indexedVertices = IndexedRDD[Int](sampleVertices)
    val indexedEdges = IndexedRDD[VertexId](graph.edges.map(e => (e.srcId, e.dstId)))
    val indexedRet1: IndexedRDD[VertexId] = indexedEdges.join(indexedVertices)((id, ed, u) => ed)

    val indexedEdgesRev = IndexedRDD[VertexId](graph.edges.map(e => (e.dstId, e.srcId)))
    val indexedRet2 = indexedEdgesRev.join(indexedVertices)((id, ed, u) => ed).map(v => (v._2, v._1))

    val edges = indexedRet1.intersection(indexedRet2).map(v => Edge[Int](v._1, v._2, 1))

    Graph.fromEdges(edges, defaultValue = 1)
  }
  */


  def sample2(graph:Graph[Int, Int], fraction: Double): Graph[Int, Int] = {

    val degreeGraph = graph.outerJoinVertices(graph.degrees) {
      case (id, u, degOpt) =>degOpt.getOrElse(0)
    }
    val neighborDegrees = Util.collectNeighborDegrees(degreeGraph, EdgeDirection.Either)
    val sparseNeighborDegrees: VertexRDD[Array[(VertexId, Int)]] = sparse.leftJoin(neighborDegrees){
      (v, vd, u) => u.getOrElse(Array())
    }
    val sparseNeighborMaxDegrees:VertexRDD[Int] = sparseNeighborDegrees.mapValues(
      v => Util.maxPairArray(v)
    )
    val maxSparseDegree:Double = sparseNeighborMaxDegrees.max()(degreeOrdering)._2.toDouble

    val sparseSample = sparseNeighborMaxDegrees.filter(v => {
      val rnd = Random.nextDouble()
      if(rnd < v._2.toDouble/maxSparseDegree){
        true
      }else{
        false
      }
    })
    val sparseSampleNeighbors = sparseSample.leftJoin(neighborDegrees){
      (v, vd, u) => u.getOrElse(Array())
    }
    val edges:RDD[Edge[Int]] = sparseSampleNeighbors.flatMap{
      case (v, n) => {
        for(n_ <- n) yield Edge[Int](v, n_._1, 1)
      }
    }
    Graph.fromEdges(edges, defaultValue = 1)
  }

}
