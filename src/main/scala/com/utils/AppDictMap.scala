package com.utils


import org.apache.spark.sql.SQLContext
import redis.clients.jedis.Jedis


/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/21
  */
object AppDictMap {
  def getAppDictMap(sQLContext: SQLContext) :Map[String, String] = {
    val prop = DBProperties.getDBConfig()
    val df = sQLContext.read.jdbc(prop.getProperty("url"), "appdict", prop)

    df.map(x => (x.getAs[String]("appid"), x.getAs[String]("appname")))
      .collect.toMap
  }

  def initRedis(sQLContext: SQLContext): Unit ={
    val jedis = new Jedis("hdp-01", 6379)
    val map = getAppDictMap(sQLContext)
    map.foreach(x => {
      val appid = "appid:" + x._1
      val appname = x._2
      jedis.hset("APP_DICT",appid,appname)
    })
    jedis.close()
  }
}
