package com.graphx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeContext, Graph, TripletFields}
import org.apache.spark.rdd.RDD

/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/26
  */
object GraphUserId {
  def graphUserId( userids: List[String], tags: List[(String, Int)] ) :Unit = {
    val conf = new SparkConf().setAppName("th").setMaster("local[*]")
    val sc = new SparkContext(conf)
    var flag = true
    var vertexList = List[(Long, String)]()
    var edgeList = List[Edge[Int]]()
//    var pointId = ""
//    for (item <- userids){
//      if (item.length > 0 && item != null ){
//        if (flag){
//          vertexList :+= (item, tags)
//          pointId = item
//          flag = false
//        }else{
//          vertexList :+= (item, List())
//        }
//      }
//    }
    var i: Long = 1L
    for(item <- userids){
      if (item.length > 0 && item != null ) {
        vertexList :+= (i, item)
        i == i + 1L
      }
    }
    val vertexRDD: RDD[(Long, String)] = sc.makeRDD(vertexList)

    for (item <- 1 to vertexList.length - 1 ){
      edgeList :+= Edge(vertexList(item)._1, vertexList(0)._1, 0)
    }
    val edgeRDD: RDD[Edge[Int]] = sc.makeRDD(edgeList)
    val graph = Graph(vertexRDD, edgeRDD)

    graph.
    graph.connectedComponents().vertices.foreach(println)
  }

}
