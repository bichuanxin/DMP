package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/23
  */
object TagsChannel extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    val channel = row.getAs[String]("channelid")
    list :+= ("CN" + channel, 1)
    list
  }
}
