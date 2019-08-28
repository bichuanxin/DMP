package com.week1

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @note 统计商圈个数
  * @Author Bi ChuanXin
  * @Date 2019/8/24
  */
object PoisContext {
  def main(args: Array[String]): Unit = {
//    1、按照pois，分类businessarea，并统计每个businessarea的总数。

//    2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量

    //初始化
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val Array(inputPath) = args
//    val inputPath = "C:\\Users\\sora\\Desktop\\json.txt"

    val res = sc.textFile(inputPath).map(json => {
      //商圈加入list集合中
      val list = mutable.ListBuffer[(String, Int)]()
      val baStr = JsonObjectUtils.getBusinessFromPois(json)
      //判断
      if (StringUtils.isNotBlank(baStr)){
        val bas = baStr.split(",")
        bas.foreach(x => list.append((x, 1)))
      }
      list.groupBy(_._1).mapValues(_.size).toList
    }).reduce((l1, l2) => {
      l1.union(l2).groupBy(_._1).mapValues(_.reduce((x, y) => (x._1, x._2 + y._2))).toList.map(_._2).filter(_._1.length > 0)
    })

    res.foreach(println)
  }

}
