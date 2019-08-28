package com.week1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * @note 统计type标签
  * @Author Bi ChuanXin
  * @Date 2019/8/24
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    //初始化
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val Array(inputPath) = args
//    val inputPath = "C:\\Users\\sora\\Desktop\\json.txt"

    val res = sc.textFile(inputPath).map(json => {
      val typeList = TagsType.makeTags(json)
      typeList.groupBy(_._1).mapValues(_.size).toList
    }).reduce((l1, l2) => {
      l1.union(l2).groupBy(_._1).mapValues(_.reduce((x, y) => (x._1, x._2 + y._2))).toList.map(_._2)
    })
    res.foreach(println)
  }
}
