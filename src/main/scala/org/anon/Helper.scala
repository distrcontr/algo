package org.anon

import java.io.File

import org.anon.distrcontr.App
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

object Helper {

  val DEBUG = false


  def logGraph[T,K](g: Graph[T,K], title:String = "") = {
    println(title)
    println("Edges:")
    g.triplets.map(
      triplet => "\t"+triplet.srcId + " owns " + triplet.attr + " of " + triplet.dstId
    ).collect.foreach(println(_))
    println("Vertices:")
    g.vertices.foreach(x=>println("\t"+x))
  }

  def mark_1(g: Graph[Char, BigDecimal], sourceId: VertexId, destId: VertexId): RDD[(VertexId, Char)] = {


    /* Marked graph:
        - weightsSum: BigDecimal
        - directlyControlled: Boolean
        - atLeastOneOut: Boolean
        - mark: Char ('K':keep, 'D': directly controlled, 'R': remove)
     */

    g.aggregateMessages[(BigDecimal, Boolean, Boolean, Char)](
      triplet => {
        val direct = triplet.attr > 0.5
        // Send to a triplet successors the edge weight (w) and direct=true if w>0.5
        triplet.sendToDst((triplet.attr, direct, false, triplet.dstAttr))

        // Send to a tripled predecessor 0 as sum ... at least one out = true
        triplet.sendToSrc((0,false, true, triplet.srcAttr))
      },

      (v1, v2) => (v1._1 + v2._1, v1._2 || v2._2, v1._3 || v2._3, v1._4)
    ).mapValues((id, value) => {

      val weightsSum = value._1
      val directlyControlled = value._2
      val atLeastOneOut = value._3
      val oldMark = value._4

      if( oldMark == 'X' || id == sourceId || id == destId ) 'X'
      else {

        // In the cleaning phase I just care of marking nodes with 'R'
        if(!atLeastOneOut || weightsSum<=0.5) {
          'R' // Note: no incoming edges implies weightsSum = 0
        } else if(directlyControlled) {
          'D'
        } else {
          'K'
        }
      }
    })


  }

  def clean(graph: Graph[Char, BigDecimal], marked_vertices:  RDD[(VertexId, Char)], sourceId: VertexId, destId: VertexId) : Graph[Char, BigDecimal] = {

    // Note: since the source node does not receive any message, it will not be in remaining_vertices

    if(DEBUG) {
      //println("\tDeleting " + marked_vertices.filter(_._2 == 'R').count()+" nodes.")
      println("\tDeleting nodes: " + marked_vertices.filter(_._2 == 'R').collect().mkString(","))
    }


    val res = Graph(marked_vertices, graph.edges).subgraph(
      vpred =  (id, attr) => attr != 'R',
      epred =  e =>  e.srcAttr!='R' || e.dstAttr!='R'
    )

    if(DEBUG)
      logGraph(res, "### RETURNING")

    res
  }

  // returns (stop, answer)
  def checkStop[T](g: Graph[(T), BigDecimal], sourceId: VertexId, destId: VertexId, startTime: Long): (Boolean, Boolean) = {

    var controls = false
    var canTerminate = false


    val s_contained = !g.vertices.filter(_._1==sourceId).isEmpty()
    val t_contained = !g.vertices.filter(_._1==destId).isEmpty()

    var (s_dir_ctrl, t_in_sum, s_ctrls_t) = (false, 0D, false)


    if(s_contained) {
      s_dir_ctrl = !g.edges.filter(e => e.srcId == sourceId && e.attr > 0.5).isEmpty()
      if(!s_dir_ctrl) {
        canTerminate = true
      }
    }

    if(t_contained) {
      t_in_sum = g.edges.filter(e => e.dstId == destId).map(_.attr).sum()
      if(t_in_sum<=0.5) {

        canTerminate = true
      }
    }

    if(s_contained && t_contained) {
      s_ctrls_t = !g.edges.filter(e => e.srcId == sourceId && e.dstId == destId && e.attr > 0.5).isEmpty()
      if (s_ctrls_t) {
        controls = true
        canTerminate = true

      }
    }

    if( canTerminate ) {
      if(controls)
        println("\nS controls T.")
      else
        println("\nS does not control T.")
      println("\nFinished after " + App.getElapsedTime(startTime) + " seconds.")
    }

    (canTerminate, controls)


  }


  def simplify(simplified_graph: Graph[(Char, List[(VertexId, BigDecimal)]), BigDecimal], sourceId: VertexId) :  Graph[(Char, List[(VertexId, BigDecimal)]), BigDecimal] = {

    var sg =  simplified_graph

    var propagated : VertexRDD[(Char, List[(VertexId, BigDecimal)])] = null

    var i=1

    do {
      if(DEBUG)
        print("\tSimplification step "+i+"\n")

      propagated = sg.aggregateMessages[(Char, List[(VertexId, BigDecimal)])](
        triplet => {

          val (headId, headType, tailId, tailType) = (triplet.srcId, triplet.srcAttr._1, triplet.dstId, triplet.dstAttr._1)
          val (headList, tailList) = (triplet.srcAttr._2, triplet.dstAttr._2)
          val weight = triplet.attr

          var message: List[(VertexId, BigDecimal)] = List()

          if(headType=='X' || headId==sourceId) triplet.sendToSrc((headType, List())) // don't remove source

          if ( headType == 'K' || headType == 'X' || headId == sourceId)
            message = List((headId, weight))
          else if (headType == 'D' && (weight > 0.5 || tailType == 'K' || tailType == 'X') )
            message =  List((headList.head._1, weight))

          triplet.sendToDst((tailType, message))
          if(DEBUG) println("\t\t"+headId+" > "+tailId+" "+message)

        },
        // reduce the weights by summing, 'or' the boolean values
        (v1, v2) => (v1._1, v1._2.union(v2._2))
      ).cache()

      sg = Graph(propagated.union(sg.vertices.filter(_._2._1=='X')), sg.edges)

      i=i+1

    } while( !propagated.filter( v=>(v._2._1=='X' ||  v._2._1=='K') && v._2._2.exists(_._1 == -1)).isEmpty() )


    val new_edges = sg.vertices.filter(v=>v._2._1=='K' || v._2._1=='X' || v._2._1 == 'N').flatMap( v => {

      val id = v._1
      val incoming: List[(VertexId, BigDecimal)] = v._2._2

      incoming.groupBy(_._1)
        .map(g => {
          val vid: VertexId = g._1
          val sum: BigDecimal = g._2.map(_._2).sum

          (vid, sum)
        }).map( inc => {

        val c_weight = inc._2
        val c_id = inc._1

        Edge[BigDecimal](c_id, id, c_weight)

      })

    }).cache()


    Graph(propagated.filter(_._2._1!='D').union(sg.vertices.filter(_._2._1=='X')), new_edges)

  }

  def mark_2(g: Graph[(Char, List[(VertexId, BigDecimal)]), BigDecimal], sourceId: VertexId, destId: VertexId) = {

    /* Marked graph:
         - weightsSum: BigDecimal
         - directlyControlled: Boolean
         - atLeastOneOut: Boolean
         - mark: Char ('K':keep, 'D': directly controlled, 'R': remove)  */

    val markedVertices = g.aggregateMessages[(BigDecimal, Boolean, Boolean, Char,  List[(VertexId, BigDecimal)])](
      triplet => {
        val direct = triplet.attr > 0.5
        // Send to a triplet successors the edge weight (w) and direct=true if w>0.5
        triplet.sendToDst((triplet.attr, direct, false, triplet.dstAttr._1, null))

        // Send to a tripled predecessor 0 as sum ... at least one out = true
        triplet.sendToSrc((0,false, true, triplet.srcAttr._1, null))
      },

      (v1, v2) => (v1._1 + v2._1, v1._2 || v2._2, v1._3 || v2._3, v1._4, null)
    ).mapValues( (id,value ) => {

      val weightsSum = value._1
      val directlyControlled = value._2
      val atLeastOneOut = value._3
      val oldMark = value._4

      val defaultList = List[(VertexId, BigDecimal)]((-1L,0))

      if( oldMark == 'X' || id == sourceId || id == destId ) ('X', defaultList)
      else {

        // In the cleaning phase I just care of marking nodes with 'R'
        if(!atLeastOneOut || weightsSum<=0.5) {
          ('R', defaultList) // Note: no incoming edges implies weightsSum = 0
        } else if(directlyControlled) {
          ('D', defaultList)
        } else {
          ('K', defaultList)
        }
      }

    }).cache()

    Graph(markedVertices, g.edges)

  }


  def toGexf[VD,ED](g:Graph[VD,ED]) : String = {
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "    <nodes>\n" +
      g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
        v._2 + "\" />\n").collect.mkString +
      "    </nodes>\n" +
      "    <edges>\n" +
      g.edges.map(e => "      <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      "    </edges>\n" +
      "  </graph>\n" +
      "</gexf>"
  }

  def strToFile(filePath: String, fileContent: String): Unit = {
    import java.io.PrintWriter
    new PrintWriter(filePath) { write(fileContent); close }
  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

}