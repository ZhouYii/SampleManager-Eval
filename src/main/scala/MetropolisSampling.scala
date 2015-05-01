import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{ListBuffer, ArraySeq, HashSet, Map}
import scala.collection.immutable.TreeMap
import scala.util.Random
import org.apache.spark.SparkContext


// In DIRECTED, we consider both incoming and outgoing edges
// In UNDIRECTED, we consider only incoming edges
object GraphType extends Enumeration {
  type GraphType = Value
  val EITHER, IN, OUT = Value
}

object EvaluationType extends Enumeration {
  type EvaluationType = Value
  val CLUSTERING_COEFFICIENTS, DEGREE_DISTRIBUTION = Value
}


object MetropolisSampling {
  
  def nodeSampling (e:Map[VertexId, HashSet[VertexId]],
                   fraction:Double): HashSet[VertexId] = {
    val vertexCount:Int = e.size
    val sampleSize:Int = math.floor(fraction*vertexCount).toInt
    // split shuffled collection
    var pair = Random.shuffle(e.keys.toList).splitAt(sampleSize)
    var vertices =  new HashSet() ++ pair._1
    vertices
  }

  def run (graph: Graph[Int,Int],
           fraction: Double, 
           numIterations: Int, 
           p: Int,
           graphType: GraphType.GraphType,
           evaluationType: EvaluationType.EvaluationType) : (HashSet[VertexId], Double) = {
    var e: Map[VertexId, HashSet[VertexId]] = Map()
    graphType match {
      case GraphType.EITHER =>  {
        e = scala.collection.mutable.Map() ++ graph.collectNeighborIds(EdgeDirection.Either).
            mapValues(element => HashSet() ++ element).collectAsMap()
      }
      case GraphType.IN => {
        e = scala.collection.mutable.Map() ++ graph.collectNeighborIds(EdgeDirection.In).
            mapValues(element => HashSet() ++ element).collectAsMap()
      }
      case GraphType.OUT => {
        e = scala.collection.mutable.Map() ++ graph.collectNeighborIds(EdgeDirection.Out).
            mapValues(element => HashSet() ++ element).collectAsMap()
      }
    }    

    val vertexCount: Int = e.size
    val sampleSize: Int = math.floor(fraction * vertexCount).toInt
    var pair = Random.shuffle(e.keys.toList).splitAt(sampleSize)
    var Sc: ArraySeq[VertexId] = new ArraySeq(0) ++ pair._1
    if (Sc.size == 0) {
      println("Error: Sample size is zero")
      return (HashSet(), 0.0)
    }
    
    var ScSet: HashSet[VertexId] = new HashSet() ++ Sc

    var eG: Map[Double, Double] = Map()
    var SEval: Map[Double, Double] = Map()
    var sampleCoeffs: Map[VertexId, Double] = Map()
    var sampleDegrees: Map[VertexId, Double] = Map()
    
    evaluationType match {
      case EvaluationType.CLUSTERING_COEFFICIENTS => {
        eG = clusteringCoefficients(ScSet, e, graphType, false)._1
        /*val degreesAndCoeffs = clusteringCoefficients(ScSet, e, graphType, true)
        SEval = degreesAndCoeffs._1
        sampleCoeffs = degreesAndCoeffs._2
        sampleDegrees = degreesAndCoeffs._3*/
        SEval = clusteringCoefficients(ScSet, e, graphType, true)._1
      }
      case EvaluationType.DEGREE_DISTRIBUTION => {
        eG = degreeDistribution(ScSet, e, false)
        SEval = degreeDistribution(ScSet, e, true)
      }
    }

    var deltaSc: Double = normDistance(SEval, eG, evaluationType)
    var Sr: ArraySeq[VertexId] = new ArraySeq(0) ++ pair._2
    var SrSet: HashSet[VertexId] = new HashSet() ++ Sr
    var Sb = Sc
    var SbSet = ScSet
    var deltaSb:Double = deltaSc
    //println(SrSet)
    //println(eG)
    //println(ScSet)
    //println(SEval)
    //println(deltaSc)
    
    var i = 0
    for(i <- 0 to numIterations - 1){
      // swap random elements in Sc and Sr
      val idxV = Math.abs(Random.nextInt) % Sc.size
      val idxW = Math.abs(Random.nextInt) % Sr.size
      val v = Sc(idxV)
      val w = Sr(idxW)
      ScSet.remove(v)
      SrSet.remove(w)
      ScSet.add(w)
      SrSet.add(v)
      val alpha = Random.nextDouble()
      
      var SnEval: Map[Double, Double] = Map()
      evaluationType match {
        case EvaluationType.CLUSTERING_COEFFICIENTS => {

          //clusteringCoefficientsIncrAll(e, eG, w, graphType)
          /*val degreesAndCoeffs = clusteringCoefficientsIncr(ScSet, e, SEval, sampleCoeffs, sampleDegrees,  w, v, graphType)
          SnEval = degreesAndCoeffs._1
          sampleCoeffs = degreesAndCoeffs._2
          sampleDegrees = degreesAndCoeffs._3*/
          SnEval = clusteringCoefficients(ScSet, e, graphType, true)._1
        }
        case EvaluationType.DEGREE_DISTRIBUTION => {
          //degreeDistributionIncrAll(e, eG, w)
          //SnEval = degreeDistributionIncr(ScSet, e, SEval, w, v)
          SnEval = degreeDistribution(ScSet, e, true)
        }
      }

      val deltaSn: Double = normDistance(SnEval, eG, evaluationType)
      if(alpha < Math.pow((deltaSc / deltaSn), p)) {
        // if better, make our changes permanent
        Sc(idxV) = w
        Sr(idxW) = v
        deltaSc = deltaSn
        SEval = SnEval
        if (deltaSc < deltaSb) {
          deltaSb = deltaSc
          Sb = Sc
          SbSet = ScSet
        }
      } else {
        // if not, roll back
        ScSet.remove(w)
        SrSet.remove(v)
        ScSet.add(v)
        SrSet.add(w)
      }
      //println(SrSet)
      //println(eG)
      //println(SbSet)
      //println(SEval)
      //println(deltaSc)
    }
    (SbSet, deltaSb)
  }
    
  // TODO: The function can be made shorter. 
  // We should take a look at it when we have some free cycles
  def clusterCoefficient (V: HashSet[VertexId],
                          E:Map[VertexId, HashSet[VertexId]],
                          v: VertexId,
                          graphType: GraphType.GraphType) : Double = {
    // set of vertexIds that have edges to v or from v
    var neighbors:HashSet[VertexId] = E.get(v).getOrElse(HashSet())
    if(V != null) {
      neighbors = neighbors.intersect(V)
    }
    
    graphType match {
      case GraphType.EITHER =>  {
        val max:Double = neighbors.size.toDouble * (neighbors.size.toDouble - 1.0)
        if (max == 0) {
          return 0.0
        }
        var count:Double = 0
        // for each neighbor w of v, we calculate the number of w's
        // neighbors that are also neighbors of v
        neighbors.foreach(w => {
          if (V == null || V.contains(w)) {
            val neighborsW: HashSet[VertexId] = E.get(w).getOrElse(HashSet())
            neighborsW.foreach(u => {
              if ((V == null || V.contains(u)) && neighbors.contains(u)) {
                count += 1
              }
            })
          }
        })
        return count.toDouble / max
      }
      case GraphType.IN | GraphType.OUT => {
        var map: Map[VertexId, HashSet[VertexId]] = Map()
        neighbors.foreach(v => map.put(v, HashSet()))
        val max: Double = neighbors.size.toDouble *(neighbors.size.toDouble - 1.0) / 2.0
        if(max == 0){
          return 0.0
        }
        var count: Double = 0
        // for each neighbor w of v, we calculate the number of w's
        // neighbors that are also neighbors of v
        neighbors.foreach(w => {
          if(V == null || V.contains(w)) {
            val incomingW: HashSet[VertexId] = E.get(w).getOrElse(HashSet()) //.intersect(V)
            incomingW.foreach(u => {
              if ((V == null || V.contains(u)) && neighbors.contains(u)
                  && !map.get(w).getOrElse(HashSet()).contains(u)
                  && !map.get(u).getOrElse(HashSet()).contains(w) ) {//incomingV.contains(u)) {
                count += 1
                map.get(w).getOrElse(HashSet()).add(u)
              }
            })
          }
        })
        return count.toDouble / max
      }
    }
  }
  
  def clusteringCoefficients (V: HashSet[VertexId],
                              E:Map[VertexId, HashSet[VertexId]],
                              graphType: GraphType.GraphType,
                              isSample: Boolean) : (Map[Double, Double], Map[VertexId, Double], Map[VertexId, Double]) = {
    var coeffs = scala.collection.mutable.Map[VertexId, Double]()
    var degrees = scala.collection.mutable.Map[VertexId, Double]()
    if (isSample) {
       V.foreach(v => {
         coeffs.put(v, clusterCoefficient(V, E, v, graphType))
         degrees.put(v, E(v).filter(element => V.contains(element)).size) 
       })
    } else {
       E.foreach(e => {
         coeffs.put(e._1, clusterCoefficient(null, E, e._1, graphType))
         degrees.put(e._1, E(e._1).size)
       })
    }
    val degreeCC = degrees.groupBy(_._2).map(e => e._1 -> (e._2.map(e => coeffs.getOrElse(e._1, 0.0))))
    return ((Map() ++ degreeCC.map(e => e._1 -> (e._2.sum / e._2.size))), coeffs, degrees)
  }
  
  def clusteringCoefficientsIncrAll (E: Map[VertexId, HashSet[VertexId]],
                                     coeffs: Map[VertexId, Double],
                                     added: VertexId,
                                     graphType: GraphType.GraphType) : Unit = {
    // udpate vertices affected by the addition of this one
    coeffs.put(added, clusterCoefficient(null, E, added, graphType))
  }

  def clusteringCoefficientsIncr (V: HashSet[VertexId],
                                  E: Map[VertexId, HashSet[VertexId]],
                                  norm: Map[Double, Double],
                                  coeffs: Map[VertexId, Double],
                                  degrees: Map[VertexId, Double],
                                  added: VertexId,
                                  removed: VertexId,
                                  graphType: GraphType.GraphType) : (Map[Double, Double], 
                                      Map[VertexId, Double], Map[VertexId, Double]) = {
    val coeffsClone = coeffs.clone()
    val degreesClone = degrees.clone()
    
    // update vertices affected by the removal of this one
    coeffsClone.remove(removed)
    degrees.remove(removed)
    E.get(removed).getOrElse(HashSet()).foreach(v => {
      if (coeffsClone.contains(v)) {
        coeffsClone.update(v, clusterCoefficient(V, E, v, graphType))
      }
      if (degreesClone.contains(v)) {
        degreesClone.update(v, degreesClone.get(v).get - 1)
      }
    })
    // update vertices affected by the addition of this one
    coeffsClone.put(added, clusterCoefficient(V, E, added, graphType))
    degrees.put(added, E(added).filter(element => V.contains(element)).size)
    E.get(added).getOrElse(HashSet()).foreach( v=> {
      if (coeffsClone.contains(v)) {
        coeffsClone.update(v, clusterCoefficient(V, E, v, graphType))
      }
      if (degreesClone.contains(v)) {
        degreesClone.update(v, degreesClone.get(v).get + 1)
      }
    })
    
    val degreeCC = degreesClone.groupBy(_._2).map(e => e._1 -> (e._2.map(e => coeffsClone.getOrElse(e._1, 0.0))))
    return ((Map() ++ degreeCC.map(e => e._1 -> (e._2.sum / e._2.size))), coeffsClone, degreesClone)
  }
  
  def degreeDistribution (V: HashSet[VertexId],
                          E: Map[VertexId, HashSet[VertexId]],
                          isSample: Boolean) : Map[Double, Double] = {
    var degrees = scala.collection.mutable.Map[VertexId, Double]()
    // compute degrees and add to map dists
    if (isSample) {
       V.foreach(v => {
         degrees.put(v, E(v).filter(element => V.contains(element)).size) 
       })
    } else {
       E.foreach(e => {
         degrees.put(e._1, E(e._1).size)
       })
    }
    // group values in the map dists based on the degree and compute, for each degree, its frequency
    val degreeDist = TreeMap(degrees.values.groupBy(e => e).map(e => e._1 -> e._2.size).toSeq:_*).iterator

    // compute the cdf of degrees
    val cdf: Map[Double, Double] = Map()
    var sum: Double = 0.0
    var count: Int = 1
    while (degreeDist.hasNext) {
      val elem = degreeDist.next()
      cdf(elem._1) =  (sum + elem._2) / count
      sum += elem._2
      count += 1
    }
    cdf
    /*V.foreach(v => {
      if (isSample) {
        dists.put(v, E(v).filter(element => V.contains(element)).size)
      } else {
        dists.put(v, E(v).size) 
      }
    })*/
  }
  
  def degreeDistributionIncr (V: HashSet[VertexId],
                              E: Map[VertexId, HashSet[VertexId]],
                              degrees: Map[VertexId, Double],
                              added: VertexId,
                              removed: VertexId) : Map[VertexId, Double] = {
    val degreesClone = degrees.clone()
    // update vertices affected by the removal of this one
    degreesClone.remove(removed)
    E.get(removed).getOrElse(HashSet()).foreach(v => {
      if (degreesClone.contains(v)) {
        degreesClone.update(v, degreesClone.get(v).get - 1)
      }
    })
    
    // update vertices affected by the addition of this one
    degreesClone.put(added, E(added).filter(element => V.contains(element)).size)
    E.get(added).getOrElse(HashSet()).foreach( v=> {
      if (degreesClone.contains(v)) {
        degreesClone.update(v, degreesClone.get(v).get + 1)
      }
    })
    degreesClone
  }
  
  def degreeDistributionIncrAll (E: Map[VertexId, HashSet[VertexId]],
                                 degrees: Map[VertexId, Double],
                                 added: VertexId) : Unit = {
    // udpate vertices affected by the addition of this one
    degrees.put(added, E(added).size)
  }
  
  def clusteringMetric (V: HashSet[VertexId],
                        E: Map[VertexId, HashSet[VertexId]],
                        cG: Map[Double, Double],
                        graphType: GraphType.GraphType) : Double = {
    clusteringCoefficientNorm(clusteringCoefficients(V, E, graphType, false)._1, cG)
  }
  
  def normDistance(cS: Map[Double, Double], 
                   cG: Map[Double, Double], 
                   evaluationType: EvaluationType.EvaluationType): Double = {
    evaluationType match {
      case EvaluationType.CLUSTERING_COEFFICIENTS => {
        return clusteringCoefficientNorm(cS, cG)
      }
      case EvaluationType.DEGREE_DISTRIBUTION => {
        return degreeDistributionNorm(cS, cG)
      }
    }
  }

  def clusteringCoefficientNorm (cdfSample: Map[Double, Double], cdfGraph: Map[Double, Double]) : Double = {
    var norm: Double = 0.0 
    var count: Int = 0
    (cdfSample.keySet ++ cdfGraph.keySet).foreach { i =>
        val sampleCount = cdfSample.getOrElse(i, 0.0)
        val graphCount = cdfGraph.getOrElse(i, 0.0)
        norm += Math.abs(sampleCount - graphCount)
        count += 1
    }
   return norm / count
    
    /*var norm: Double = 0.0
    cS.foreach(vc => {
      norm += Math.abs(vc._2 - cG.get(vc._1).getOrElse(0.0))
    })
    norm / cS.size*/
  }
  
  def degreeDistributionNorm (cdfSample: Map[Double, Double], cdfGraph: Map[Double, Double]) : Double = {    
    var norm: Double = 0.0 
    (cdfSample.keySet ++ cdfGraph.keySet).foreach { i =>
        val sampleCount = cdfSample.getOrElse(i, 0.0)
        val graphCount = cdfGraph.getOrElse(i, 0.0)
        norm = Math.max(norm, Math.abs(sampleCount - graphCount))
    }
   return norm
   /*val cSDegrees = TreeMap(cS.values.groupBy(e => e).map(e => e._1 -> e._2.size).toSeq:_*).iterator
    println("SampleS: " + TreeMap(cS.values.groupBy(e => e).map(e => e._1 -> e._2.size).toSeq:_*))
    val cdfSample: Map[Double, Int] = Map()
    var prev: Double = -1.0
    while (cSDegrees.hasNext) {
      val elem = cSDegrees.next()
      cdfSample(elem._1) =  elem._2 + cdfSample.getOrElse(prev, 0)
      prev = elem._1
    }
    
    val cGDegrees = TreeMap(cG.values.groupBy(e => e).map(e => e._1 -> e._2.size).toSeq:_*).iterator
    println("GraphS: " + TreeMap(cG.values.groupBy(e => e).map(e => e._1 -> e._2.size).toSeq:_*))
    val cdfGraph: Map[Double, Int] = Map()
    prev = -1.0
    while (cGDegrees.hasNext) {
      val elem = cGDegrees.next()
      cdfGraph(elem._1) =  elem._2 + cdfGraph.getOrElse(prev, 0)
      prev = elem._1
    }*/
    /*var norm: Double = 0.0
    cS.foreach(vc => {
      norm = Math.max(norm, Math.abs(vc._2 - cG.get(vc._1).getOrElse(0.0)))
    })
    norm*/
  }
  
  /*def runWithDegreeDistribution (graph: Graph[Int,Int],
                                 fraction: Double, 
                                 numIterations: Int, 
                                 p: Int,
                                 graphType: GraphType.GraphType) : (HashSet[VertexId], Double) = {
    var e: Map[VertexId, HashSet[VertexId]] = Map()
    graphType match {
      case GraphType.EITHER =>  {
        e = scala.collection.mutable.Map() ++ graph.collectNeighborIds(EdgeDirection.Either).mapValues(element => HashSet() ++ element).collectAsMap()
      }
      case GraphType.IN => {
        e = scala.collection.mutable.Map() ++ graph.collectNeighborIds(EdgeDirection.In).mapValues(element => HashSet() ++ element).collectAsMap()
      }
      case GraphType.OUT => {
        e = scala.collection.mutable.Map() ++ graph.collectNeighborIds(EdgeDirection.Out).mapValues(element => HashSet() ++ element).collectAsMap()
      }
    }     

    val vertexCount: Int = e.size
    val sampleSize: Int = math.floor(fraction * vertexCount).toInt
    var pair = Random.shuffle(e.keys.toList).splitAt(sampleSize)
    var Sc: ArraySeq[VertexId] = new ArraySeq(0) ++ pair._1
    var ScSet: HashSet[VertexId] = new HashSet() ++ Sc

    // degree distributions
    var dG: Map[Double, Int] = degreeDistribution(ScSet, e, false)
    var SdDist: Map[Double, Int] = degreeDistribution(ScSet, e, true)

    var deltaSd: Double = degreeDistributionNorm(SdDist, dG)
    var Sr: ArraySeq[VertexId] = new ArraySeq(0) ++ pair._2
    var SrSet: HashSet[VertexId] = new HashSet() ++ Sr
    var Sb = Sc
    var SbSet = ScSet
    var deltaSb:Double = deltaSd
    var i = 0
    for(i <- 0 to numIterations - 1){
      // swap random elements in Sc and Sr
      val idxV = Math.abs(Random.nextInt) % Sc.size
      val idxW = Math.abs(Random.nextInt) % Sr.size
      val v = Sc(idxV)
      val w = Sr(idxW)
      ScSet.remove(v)
      SrSet.remove(w)
      ScSet.add(w)
      SrSet.add(v)
      val alpha = Random.nextDouble()
      //degreeDistributionIncrAll(e, dG, w)
      val SnDist: Map[Double, Int] = degreeDistribution(ScSet, e, true)
      val deltaSn: Double = degreeDistributionNorm(SnDist, dG)
      if(alpha < Math.pow((deltaSd / deltaSn), p)) {
        // if better, make our changes permanent
        Sc(idxV) = w
        Sr(idxW) = v
        deltaSd = deltaSn
        SdDist = SnDist
        if (deltaSd < deltaSb) {
          deltaSb = deltaSd
          Sb = Sc
          SbSet = ScSet
        }
      } else {
        // if not, roll back
        ScSet.remove(w)
        SrSet.remove(v)
        ScSet.add(v)
        SrSet.add(w)
      }
    }
    (SbSet, deltaSb)
  }*/
  
  def degreeDistributionT (G: Graph[Int,Int],
                           V: RDD[(VertexId, Int)]) : Double = {
    // get in degrees of each vertex
    val degrees = V.leftOuterJoin(G.inDegrees).map {
      case (id, (attr, inDegOpt)) => (id, inDegOpt.getOrElse(0))
    }
    0.0
  }

  def clusteringCoefficientsT (graph: Graph[Int, Int]) : Graph[Double, Int] = {
    val coeffs = graph.outerJoinVertices(graph.inDegrees) {
      case (id, attr, inDegOpt) => inDegOpt.getOrElse(0)
    }.outerJoinVertices(graph.outDegrees) {
      case (id, attr, outDegOpt) => attr + outDegOpt.getOrElse(0)
    }.outerJoinVertices(graph.triangleCount().vertices) {
      case (id, attr, count) =>
        if (attr > 1) {
          2.0 * count.getOrElse(0) / (attr * (attr - 1))
        } else {
          0.0
        }
    }
    coeffs
  }

  def L1NormT (sc: SparkContext, 
               sample: Graph[Double, Int], 
               original: Graph[Double, Int]) : Double = {
    val absDiff = sample.outerJoinVertices(original.vertices) {
      case (id, attr, attr2) => {
        math.abs(attr2.getOrElse(0.0) - attr)
      }
    }.vertices

    val accum = sc.accumulator(0.0)
    absDiff.foreach(v => accum += v._2)
    accum.value
  }

  def metropolisSampleT (sc: SparkContext,
                         graph: Graph[Int,Int],
                         fraction: Double,
                         numIterations: Int) : Graph[Int, Int] = {
    val clusterG = clusteringCoefficientsT(graph)
    clusterG.cache()
    val vertexCount = graph.vertices.count
    val sampleSize = math.floor(fraction*vertexCount).toInt
    var G_c:Graph[Int, Int] = null
    var D_c:Double = 0.0
    var G_b:Graph[Int, Int] = NodeSampling().sample(graph, fraction)//Sampling.nodeSampling(sc, graph, fraction)
    G_b.cache()
    var D_b:Double = L1NormT(sc, clusteringCoefficientsT(G_b), clusterG)
    G_b.vertices.unpersist(false)
    G_b.vertices.unpersist(false)
    var i = 0
    for(i <- 0 to numIterations) {
      G_c = NodeSampling().sample(graph, fraction)
      G_c.cache()
      D_c = L1NormT(sc, clusteringCoefficientsT(G_c), clusterG)
      println(s"Db: ${D_b} Dc: ${D_c}")
      if(D_c < D_b){
        G_b = G_c
        D_b = D_c
      }else{
        G_c.vertices.unpersist(false)
        G_c.edges.unpersist(false)
      }
    }
    clusterG.vertices.unpersist(false)
    clusterG.edges.unpersist(false)
    G_b
  }
  
  // This part of code used to be Metropolis Sampling for undirected graphs.
  // Keeping it to check the current version with the previous version
  /*
  def run(e: Map[VertexId, HashSet[VertexId]],
          fraction: Double, 
          numIterations: Int, 
          p: Int,
          graphType: GraphType.GraphType) : (HashSet[VertexId], Double) = {
    val vertexCount:Int = e.size
    val sampleSize:Int = math.floor(fraction*vertexCount).toInt
    var pair = Random.shuffle(e.keys.toList).splitAt(sampleSize)
    var Sc:ArraySeq[VertexId] = new ArraySeq(0) ++ pair._1
    var ScSet:HashSet[VertexId] = new HashSet() ++ Sc

    // clustering coefficients
    var cG: Map[VertexId, Double] = clusteringCoefficientsAll(ScSet, e)
    var ScCoeffs: Map[VertexId, Double] = clusteringCoefficients(ScSet, e)

    var deltaSc: Double = L1Norm(ScCoeffs, cG)
    var Sr:ArraySeq[VertexId] = new ArraySeq(0) ++ pair._2
    var SrSet:HashSet[VertexId] = new HashSet() ++ Sr
    var Sb = Sc
    var SbSet = ScSet
    var deltaSb:Double = deltaSc
    var i = 0
    for(i <- 0 to numIterations-1){
      // swap random elements in Sc and Sr
      val idxV = Math.abs(Random.nextInt) % Sc.size
      val idxW = Math.abs(Random.nextInt) % Sr.size
      val v = Sc(idxV)
      val w = Sr(idxW)
      ScSet.remove(v)
      SrSet.remove(w)
      ScSet.add(w)
      SrSet.add(v)
      val alpha = Random.nextDouble()
      clusteringCoefficientsIncrAll(e, cG, w)
      val SnCoeffs: Map[VertexId, Double] = clusteringCoefficientsIncr(ScSet, e, ScCoeffs, w, v)
      val deltaSn: Double = L1Norm(SnCoeffs, cG)
      if(alpha < Math.pow((deltaSc/deltaSn), p)) {
        // if better, make our changes permanent
        Sc(idxV) = w
        Sr(idxW) = v
        deltaSc = deltaSn
        ScCoeffs = SnCoeffs
        if(deltaSc < deltaSb){
          deltaSb = deltaSc
          Sb = Sc
          SbSet = ScSet
        }
      }else{
        // if not, roll back
        ScSet.remove(w)
        SrSet.remove(v)
        ScSet.add(v)
        SrSet.add(w)
      }
    }
    (SbSet, deltaSb)
  }
  
  def clusterCoefficient(V: HashSet[VertexId],
                         E:Map[VertexId, HashSet[VertexId]],
                         v: VertexId) : Double = {
    // set of vertexIds that have edges to v
    var incomingV:HashSet[VertexId] = E.get(v).getOrElse(HashSet())
    var map:Map[VertexId, HashSet[VertexId]] = Map()
    incomingV.foreach(v=>map.put(v, HashSet()))
    if(V != null){
      incomingV = incomingV.intersect(V)
    }
    val max:Double = incomingV.size.toDouble *(incomingV.size.toDouble - 1.0)/2.0
    if(max == 0){
      return 0.0
    }
    var count:Double = 0
    // for each neighbor w of v, we calculate the number of w's
    // neighbors that are also neighbors of v
    incomingV.foreach(w => {
      //var incomingW = E.get(w).getOrElse(HashSet())//.filter(_ > w)
      if(V == null || V.contains(w)) {
        val incomingW: HashSet[VertexId] = E.get(w).getOrElse(HashSet()) //.intersect(V)
        incomingW.foreach(u => {
          if ((V == null || V.contains(u)) && incomingV.contains(u)
            && !map.get(w).getOrElse(HashSet()).contains(u)
            && !map.get(u).getOrElse(HashSet()).contains(w) ){//incomingV.contains(u)) {
            count += 1
            map.get(w).getOrElse(HashSet()).add(u)
          }
        })
      }
    })
    count.toDouble/max
  }

    def clusterCoefficient2(V: HashSet[VertexId],
                         E:Map[VertexId, HashSet[VertexId]],
                         v: VertexId) : Double = {
    // set of vertexIds that have edges to v
    var incomingV:HashSet[VertexId] = E.get(v).getOrElse(HashSet())
    if(V != null){
      incomingV = incomingV.intersect(V)
    }
    val max:Double = incomingV.size.toDouble *(incomingV.size.toDouble - 1.0)/2.0
    if(max == 0){
      return 0.0
    }
    var count:Double = 0
    // for each neighbor w of v, we calculate the number of w's
    // neighbors that are also neighbors of v
    incomingV.foreach(w => {
      var incomingW = E.get(w).getOrElse(HashSet())
      if(V != null){
        incomingW = incomingW.intersect(V)
      }
      count += incomingW.intersect(incomingV).size
    })
    count.toDouble/max
  }

  def clusteringCoefficientsIncrAll(E:Map[VertexId, HashSet[VertexId]],
                                    coeffs:Map[VertexId, Double],
                                    added:VertexId) : Unit = {
    // udpate vertices affected by the addition of this one
    coeffs.put(added, clusterCoefficient(null, E, added))
  }

  def clusteringCoefficientsIncr(V: HashSet[VertexId],
                                 E:Map[VertexId, HashSet[VertexId]],
                                 coeffs:Map[VertexId, Double],
                                 added:VertexId,
                                 removed:VertexId) : Map[VertexId, Double] = {
    val coeffClone = coeffs.clone()
    // update vertices affected by the removal of this one
    coeffClone.remove(removed)
    E.get(removed).getOrElse(HashSet()).foreach(v=>{
      if(coeffClone.contains(v)){
        coeffClone.update(v, clusterCoefficient(V, E, v))
      }
    })
    // update vertices affected by the addition of this one
    coeffClone.put(added, clusterCoefficient(V, E, added))
    E.get(added).getOrElse(HashSet()).foreach(v=>{
      if(coeffClone.contains(v)){
        coeffClone.update(v, clusterCoefficient(V, E, v))
      }
    })
    coeffClone
  }
  
   def clusteringCoefficients(V: HashSet[VertexId],
                             E:Map[VertexId, HashSet[VertexId]]) : Map[VertexId, Double] = {
    var coeffs = scala.collection.mutable.Map[VertexId, Double]()
    V.foreach(v => {
      coeffs.put(v, clusterCoefficient(V, E, v))
    })
    coeffs
  }

  def clusteringCoefficientsAll(V: HashSet[VertexId],
                                E:Map[VertexId, HashSet[VertexId]]) : Map[VertexId, Double] = {
    var coeffs = scala.collection.mutable.Map[VertexId, Double]()
    V.foreach(v => {
      coeffs.put(v, clusterCoefficient(null, E, v))
    })
    coeffs
  }*/
}
