package com.Tags

import com.utils.{AmapUtils, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/24
  */
object TagsBusiness extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    var longDou: Double = 0
    var latDou: Double = 0
    if (StringUtils.isNoneBlank(long)){
      longDou = long.toDouble
    }
    if (StringUtils.isNoneBlank(lat)){
      latDou = lat.toDouble
    }

    val buniessStr: String = AmapUtils.getBusinessFromAmap(longDou, latDou)
    if (StringUtils.isNotBlank(buniessStr)){
      val businesses = buniessStr.split(",")
      list :+= ("BN"+businesses, 1)
    }
    list
  }
}
