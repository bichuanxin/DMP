package com.graphx

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/26
  */
object Graph_test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")

    val sc = new SparkContext(conf)
    // 构建点的集合
    val vertexRDD: RDD[(Long, (String, Int))] = sc.makeRDD(Seq(
      (1L, ("sora", 15)),
      (2L, ("kirito", 16)),
      (6L, ("asuna", 18)),
      (9L, ("shiro", 12)),
      (133L, ("yui", 10)),
      (138L, ("satomi", 28)),
      (16L, ("nezuko", 13)),
      (44L, ("tanjiro", 16)),
      (21L, ("inosuke", 15)),
      (5L, ("高斯林", 60)),
      (7L, ("奥德斯基", 55)),
      (158L, ("码云", 55))
    ))
    // 构建边的集合
    val edge: RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))
    // 构件图
    val graph = Graph(vertexRDD, edge)
    // 取出每个边上的最大顶点
    val vertices = graph.connectedComponents().vertices
    vertices.foreach(println)
    vertices.join(vertexRDD).map{
      case(userId, (conId,(name,age))) =>{
        (conId,List(name,age))
      }
    }.foreach(println)


  }
}
