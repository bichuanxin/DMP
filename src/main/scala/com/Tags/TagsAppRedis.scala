package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/23
  */
object TagsAppRedis extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    //获取row
    val row = args(0).asInstanceOf[Row]
    //获取redis连接
    val conn = args(1).asInstanceOf[Jedis]

    val appid = row.getAs[String]("appid")
    var appname = row.getAs[String]("appname")

    if (appname.equals("其他")){
      appname = conn.hget("APP_DICT",appid)

//      appname = appDict.getOrElse(appid, "None")
      if (appname.equals("None")){
        return list
      }else{
        list :+= ("APP" + appname, 1)
      }
    }else{
      list :+= ("APP" + appname, 1)
    }

    list
  }
}
