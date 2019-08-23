package com.Tags

import com.utils.{AppDictMap, RedisPool, TagsUtils}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * @note 上下文标签
  * @Author Bi ChuanXin
  * @Date 2019/8/23
  */
object TagsContextRedis {
  def main(args: Array[String]): Unit = {
    if (args.length != 4){
      println("目录不匹配")
      sys.exit()
    }

    val Array(inputPath, outputPath,a ,b) = args
    //创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    //读取数据
    val df = sQLContext.read.parquet(inputPath)

//    AppDictMap.initRedis(sQLContext)
    // 过滤符合id的数据
    val tag = df.filter(TagsUtils.OneUserId)
      .mapPartitions(part =>{
        val conn: Jedis = RedisPool.getConnection()

        val parted = part.map(row => {
          //取出用户id
          val userid = TagsUtils.getOneUserId(row)
          // 接下来通过row数据 打上 所有标签（按照需求）
          //广告类型标签
          val adTags = TagsAd.makeTags(row)
          //app标签
          val appTags = TagsAppRedis.makeTags(row, conn)
          //渠道标签
          val channelTags = TagsChannel.makeTags(row)
          //设备标签
          val deviceTags = TagsDevice.makeTags(row)
          //关键字标签
          val keywordsTags = TagsKeywords.makeTags(row)
          // 地域标签
          val locationTags = TagsLocation.makeTags(row)
          //整合
          val tags: List[(String, Int)] = adTags.union(appTags)
            .union(channelTags)
            .union(deviceTags)
            .union(keywordsTags)
            .union(locationTags)
          (userid, tags)
        })
        conn.close()
        parted
      })
    // 聚合
//    val tag1 = tag.reduceByKey((l1, l2) => {
//      val list: List[(String, Int)] = l1.union(l2)
//      val grouped: Map[String, List[(String, Int)]] = list.groupBy(_._1)
//      val sum: Map[String, Int] = grouped.mapValues(_.size)
//      val value: List[(String, Int)] = sum.toList
//      value
//    })
    tag.collect.foreach(println)
  }
}
