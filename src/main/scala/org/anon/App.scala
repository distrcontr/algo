package org.anon.distrcontr

import org.anon.Helper
import org.apache.spark._
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.sellmerfud.optparse._

import scala.collection.mutable.ListBuffer

object App {

  val N = 5
  val M = 4

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  var lastRecordedTime: Double = 0
  val times:  scala.collection.mutable.ListBuffer[Double]= scala.collection.mutable.ListBuffer[Double]()


  // Command Line Options
  case class Config( folder:     String   = null,
                     source:   Long     = 1L,
                     target:   Long     = 11L,
                     logInfo:  Boolean  = false,
                     minPartitions: Int  = 2,
                     inContext: Boolean = false,
                     cache: Boolean = false,
                     debug: Boolean = false,
                     testing: Boolean = false,
                     skipCleaning: Boolean = false,
                     xmlPath: String =  "")

  def getElapsedTime(startTime: Double) : Double = {
    val cur = (System.nanoTime - startTime) / 1e9d
    //val cur_str = cur formatted "%.2f s"
    val diff = cur - lastRecordedTime
    val diff_str = diff formatted  "%.3f"

    lastRecordedTime = cur
    diff_str.toDouble

    //cur_str+ " (+"+diff_str+")"
  }

  // Result
  case class Step(id: Int, deleted: List[Long])
  case class Result(var answer: Boolean = false, var phase1: ListBuffer[Step] = ListBuffer(), var phase2: ListBuffer[Step] = ListBuffer(), var reduced: Graph[Char, BigDecimal] = null, var time: Double = 0)


  def run(config: Config, g: Graph[Char, BigDecimal], partitionName: String, sc: SparkContext): Result = {

    println("\nProcessing "+partitionName)

    lastRecordedTime = 0

    var graph = g//.cache()
    val result = Result()

    val startTime = System.nanoTime

    val DEBUG    = config.debug
    val TESTING  = config.testing

    var sourceId = config.source
    var destId   = config.target

    val out =   config.xmlPath



    var i = 1 // STEP COUNTER

    //Helper.strToFile(out+"original.xml", Helper.toGexf(graph))

    if(DEBUG)
      println("The original graph contains "+graph.vertices.count()+" vertices")

    // CHECK EARLY STOP
    val (canStop, answer) = Helper.checkStop(graph, sourceId, destId, startTime)
    if(canStop) {
      result.answer = answer
      result.reduced = graph
      result.time =  (System.nanoTime - startTime) / 1e9d
      return result
    }

    /* ====== PHASE 1 ===== */

    // Inital MARK
    var marked_vertices: RDD[(VertexId, Char)] = Helper.mark_1(graph, sourceId, destId)//.mapValues(v=>{println("mark1_"+i+" "+N);v}).cache()
    if(config.cache) marked_vertices = marked_vertices.cache()

    var cached_marked: RDD[(VertexId, Char)] = null

    // Stop when no more 'R' nodes are contained in the graph
    if(!config.skipCleaning)
      while ( !marked_vertices.filter(v => v._2=='R').isEmpty()) {

        if(cached_marked!=null) {
          println("uncaching marked_vertices")
          cached_marked.unpersist()
        }

        println("Phase 1 : Iteration "+i+" ...")


        // CLEAN
        graph = Helper.clean(graph, marked_vertices, sourceId, destId)//.cache()
        //Helper.strToFile(out+"step1_"+i+".xml", Helper.toGexf(graph))

        // CHECK EARLY STOP
        val (canStop, answer) = Helper.checkStop(graph, sourceId, destId, startTime)
        if(canStop) {
          result.answer = answer
          result.reduced = graph
          result.time =  (System.nanoTime - startTime) / 1e9d
          return result
        }

        //Helper.logGraph(graph)


        if(DEBUG)
          println("The cleaned graph contains "+graph.vertices.count()+" vertices")

        if(TESTING)
          result.phase1 += Step(i, marked_vertices.filter(_._2 == 'R').map(_._1.toLong).collect().toList)

        // MARK
        if(config.cache) cached_marked = marked_vertices
        marked_vertices = Helper.mark_1(graph, sourceId, destId)
        if(config.cache) marked_vertices = marked_vertices.cache()


        i=i+1

      }

    //Helper.logGraph(graph)

    if(DEBUG)
      println("At the end of the first phase there are "+marked_vertices.count()+" vertices")

    /* ====== PHASE 2 ===== */


    i=1 // re-initialize step counter
    // change representation
    var newVertices: RDD[(VertexId, (Char, List[(VertexId, BigDecimal)]))] = marked_vertices.mapValues(label => {
      (label , List((-1, 0)))
    })
    var simplifiedGraph:  Graph[(Char, List[(VertexId, BigDecimal)]), BigDecimal]  = Graph(newVertices, graph.edges)
    if(config.cache) simplifiedGraph = simplifiedGraph.cache()


    var cached_simplified: Graph[(Char, List[(VertexId, BigDecimal)]), BigDecimal] = null



    while( !simplifiedGraph.vertices.filter(v=>v._2._1=='D').isEmpty() ) {

      if(config.cache && cached_simplified!=null) {
        println("uncaching simplified")
        cached_simplified.unpersist()
      }

      println("Phase 2 : Iteration "+i+" ...")

      // CHECK EARLY STOP
      val (canStop, answer) = Helper.checkStop(simplifiedGraph, sourceId, destId, startTime)
      if(canStop) {
        result.answer = answer;
        result.reduced = simplifiedGraph.mapVertices((id, a)  => {a._1})
        result.time =  (System.nanoTime - startTime) / 1e9d
        return result
      }

      if(config.cache && i==1)
        marked_vertices.unpersist()

      // SIMPLIFY
      simplifiedGraph  = Helper.simplify(simplifiedGraph, sourceId)

      if(DEBUG)
        println("After simplification there are "+simplifiedGraph.vertices.count()+" vertices")


      // MARK
      cached_simplified= simplifiedGraph
      simplifiedGraph = Helper.mark_2(simplifiedGraph, sourceId, destId)
      if(config.cache) simplifiedGraph = simplifiedGraph.cache()

      if(TESTING)
        result.phase2 += Step(i, simplifiedGraph.vertices.filter(_._2._1 == 'D').map(_._1.toLong).collect().toList)


      i=i+1


    }


    // PRINT FINAL SOLUTION
    result.answer =  Helper.checkStop(simplifiedGraph, sourceId, destId, startTime)._2
    result.reduced = simplifiedGraph.mapVertices((id, a)  => {a._1})
    result.time =  (System.nanoTime - startTime) / 1e9d

    val parallelRedTime = (System.nanoTime - startTime) / 1e9d

    println("Processing of "+partitionName+" took "+result.time)
    result

  }


  def main(args: Array[String]): Unit = {

    var parser:OptionParser[Config]  = null
    // Command Line Options Parser
    val appConf: Config = try {
      parser = new OptionParser[Config] {
        banner = "Distributed Control CLI"
        separator("Main Options:")

        reqd[String]("-f", "--folder", "Path of the folder containing the partitioned graph.")
          { (v, cfg) => cfg.copy(folder = v) }

        reqd[Long]("-s", "--source", "Source node ID.")
          { (v, cfg) =>  cfg.copy(source = v) }

        reqd[Long]("-t", "--target", "Target node ID.")
          { (v, cfg) =>  cfg.copy(target = v) }

        bool("-l", "--log", "Set org/akka log level to INFO")
        { (v, cfg) =>  cfg.copy(logInfo = true) }

        optl[Int]("-p", "--minPartitions", "Minimum number of partitions in which the file is split.")
          { (v, cfg) =>  cfg.copy(minPartitions = v getOrElse 2) }

        bool("-x", "--inContext", "set if you run with spark-submit")
        { (v, cfg) =>  cfg.copy(inContext = true) }

        bool("-c", "--cache", "cache RDDs")
        { (v, cfg) =>  cfg.copy(cache = true) }

        bool("-k", "--skipCleaning", "skip cleaning phase")
        { (v, cfg) =>  cfg.copy(skipCleaning = true) }

        bool("-d", "--debug", "Enable DEBUG mode")
        { (v, cfg) =>  cfg.copy(debug = true) }

        bool("-v", "--ts", "Enable TESTING mode")
        { (v, cfg) =>  cfg.copy(testing = true) }

        optl[String]("-o", "--xmlPath", "Local folder for storing the xml representation of the graph.")
          { (v, cfg) => cfg.copy(xmlPath = v getOrElse "" ) }

      }
      parser.parse(args, Config())
    }
    catch { case e: OptionParserException => println(e.getMessage); sys.exit(1) }

    println("config: " + appConf)

    if(appConf.logInfo) {
      Logger.getLogger("org").setLevel(Level.INFO)
      Logger.getLogger("akka").setLevel(Level.INFO)
    }


   if(appConf.folder==null) {
     println(parser)
     sys.exit(1)
   }

    val partitions = Helper.getListOfFiles(appConf.folder)

    println(partitions.length + " partitions found.")


    var mergedGraphV = scala.collection.mutable.ListBuffer[(VertexId, Char)]()
    var mergedGraphE = scala.collection.mutable.ListBuffer[Edge[BigDecimal]]()

    // Set Spark Context
    val sparkConf = new SparkConf().setAppName("DisCont")
    if(!appConf.inContext) sparkConf.setMaster("local[*]")
    var sc = new SparkContext(sparkConf)


    for( partition <- partitions ) {


      // Build the initial graph
      val file = sc.textFile(partition.getAbsolutePath, appConf.minPartitions)
      val marked_edges: RDD[Edge[(Double, Char)]] = file.map( line => {
        val l = line.split(",")
        Edge(l(0).toLong, l(1).toLong, (l(2).toDouble, l(3).head))
      }).cache()

      val nodes: RDD[(VertexId, Char)] = marked_edges.flatMap(e => {

        e.attr._2 match {
          case 'N' => Seq( (e.srcId, 'N'), (e.dstId, 'N'))
          case 'I' => Seq( (e.dstId, 'X')) // the incoming is the destination
          case 'V' => Seq( (e.srcId, 'N'), (e.dstId, 'X'))
        }
      }).groupBy(_._1).map(v => {

        val label =  v._2.reduce( (el1, el2) => if(el1._2=='X' || el2._2=='X') (1,'X') else (1,'N'))._2
        (v._1, label)
      })

      val edges: RDD[Edge[BigDecimal]] = marked_edges.map(e=>Edge(e.srcId, e.dstId, e.attr._1))


      var graph = Graph(nodes, edges)//.mapVertices((id,v)=>{println("vertices "+N); v})


      val result = run(appConf, graph, partition.getName, sc)


      var reduced = result.reduced

      if(appConf.cache)
        reduced = reduced.cache()


      times += result.time

      mergedGraphV = mergedGraphV.union(reduced.vertices.mapValues(x=>'N').collect.toList)
      mergedGraphE = mergedGraphE.union(reduced.edges.collect().toList)
      reduced.unpersist()

    }

    println("Processing at coordinator site.")

    val maxParallelTime = times.max


    val v: RDD[(VertexId, Char)] = sc.parallelize(mergedGraphV, appConf.minPartitions)
    val e: RDD[Edge[BigDecimal]] = sc.parallelize(mergedGraphE, appConf.minPartitions)


    val MGraph = Graph(v, e)
    val result = run(appConf, MGraph, "MGraph", sc)
    times += result.time

    val responseTime = maxParallelTime + times.last
    val cost = times.sum

    println("\nResponse time: "+responseTime+" s")
    println("Cost: "+cost)

  }


}