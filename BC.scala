import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import sun.util.resources.cldr.om.LocaleNames_om
import scala.compat.Platform.currentTime

import scala.collection.mutable
import scala.collection.immutable.Map
/**
  * Created by troy on 3/7/16.
  */

/**
  * @version 2.0
  * @author Sixun Ouyang
  * */

object BC {
  var V: Array[Int] = Array()
  var B_C: List[Double] = List()
  var T_B_C: List[Double] = List()

  def main(args: Array[String]) {
    val total_time: Double= currentTime

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf()
      .setMaster("local").setAppName("BC")

    val sc = new SparkContext(conf)
    val path: String = "/home/troy/Clustering Coeffiency/data/s11.csv"

    val graph = loadData(path, sc)

    val neighbors = graph.collectNeighborIds(edgeDirection = EdgeDirection.Out).collect()
    println("neighbors get ready")
    V = graph.vertices.map{x => x._1.toInt}.collect()
    B_C = List.fill(V.length)(0.0)
    T_B_C = List.fill(V.length)(0.0)

    val neighbor_map = (neighbors.map(_._1), neighbors.map(_._2)).zipped.map(_ -> _).toMap

    val r = calculateBC(graph, neighbor_map)
    println("tbc " + T_B_C)
    println("vertices: " + graph.vertices.count())
    println("edges: " + graph.edges.count())
    val time_cost: Double = currentTime
    println("total time cost: " + (time_cost - total_time) / 1000)
  }

  /**
    * load and convert data into a desier format
    * */
  def loadData(path: String, sc: SparkContext): Graph[Int, Int] = {
    /**load from file*/
    val raw: RDD[Edge[Int]] = sc.textFile(path).map{ s =>
      val parts = s.split("\\s+")
      Edge(parts(0).toLong, parts(1).toLong, 0)
    }.distinct
    val convert : RDD[Edge[Int]] = raw.filter{ s =>
      s.srcId != s.dstId
    }

    /**build graph*/
    val raw_graph : Graph[Int, Int] =
      Graph.fromEdges(convert, 0)
    raw_graph.cache()
    raw_graph
  }

  /**
    * calculate the betweenness centrality of each vertex
    * */
  def calculateBC(graph: Graph[Int, Int], neighbors: Map[Long, Array[VertexId]]): Graph[PartitionID, PartitionID] = {

    val temp = graph.pregel(0, maxIterations = 1)(
      (id, attr, msg) => {
        //println("pregel")
        val s = id.toInt
        val s_index: Int = V.indexOf(s)
        /**
          * generate a Stack, a Queue, the process list (should be a map), sigma, distance, and delta
          **/
        val S = mutable.Stack[Int]()
        val Q = mutable.Queue[Int]()
        var P = Map[Long, Array[Int]]()
        /**
          * get the current id to s
          * set sigma and distance of each vertices
          * if verex is the current vertex set sigma to 1 and distance to 0 (default before pregel)
          * if not, set sigma to 0, distance to -1 (default before pregel)
          **/
        var SIGMA = List.fill(V.length)(0)
        var DISTANCE = List.fill(V.length)(-1)
        var DELTA = List.fill(V.length)(0)

        SIGMA = SIGMA.updated(s_index, 1)
        DISTANCE = DISTANCE.updated(s_index, 0)

        /**
          * enqueue s into Q
          **/
        Q.enqueue(s)
        while (Q.isEmpty.equals(false)) {
          val v = Q.dequeue()
          val v_index = V.indexOf(v)
          S.push(v)

          //val neighbor_id = m(v)

          val neighbor_id = neighbors(v)

          for (w <- neighbor_id) {
            val w_index = V.indexOf(w.toInt)
            /**
              * found for the first time
              **/
            if (DISTANCE(w_index) < 0) {
              /**
                * put neighbor into Q
                **/
              Q.enqueue(w.toInt)

              /**
                * change distance between w and v
                **/
              DISTANCE = DISTANCE.updated(w_index, DISTANCE(v_index) + 1)
            }
            if (DISTANCE(w_index) == DISTANCE(v_index) + 1) {
              /**
                * change sigma
                **/
              SIGMA = SIGMA.updated(w_index, SIGMA(w_index) + SIGMA(v_index))

              if (P.contains(w)) {
                val value_ = P(w) :+ v
                P += (w -> value_)
              }
              else
                P += (w -> Array(v))
            }
          }
        }

        while (S.isEmpty.equals(false)) {
          val w = S.pop()
          val w_index = V.indexOf(w)
          if (P.contains(w)) {
            for (v <- P(w)) {
              val v_index = V.indexOf(v)
              if (SIGMA(w_index) != 0) {
                DELTA = DELTA.updated(v_index, DELTA(v_index) + (SIGMA(v_index) / SIGMA(w_index)) * (1 + DELTA(w_index)))
              }
            }
          }
          if (w != s) {
            B_C = B_C.updated(w_index, B_C(w_index) + DELTA(w_index))
          }
        }
        T_B_C = T_B_C.updated(s_index, B_C(s_index))
        println("this is vertex " + s)
        println("B_C " + B_C)
        msg
      },
      edge => Iterator.empty,
      (a, b) => a
    )
    graph
  }

}




