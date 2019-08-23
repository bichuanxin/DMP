package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/23
  */
object TagsDevice extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    val os = row.getAs[Int]("client")
    val net = row.getAs[String]("networkmannername")
    val isp = row.getAs[String]("ispname")

    os match {
      case 1 => list :+= ("D00010001", 1)
      case 2 => list :+= ("D00010002", 1)
      case 3 => list :+= ("D00010003", 1)
      case _ => list :+= ("D00010004", 1)
    }

    net match {
      case "Wifi" => list :+= ("D00020001 ", 1)
      case "4G" => list :+= ("D00020002 ", 1)
      case "3G" => list :+= ("D00020003 ", 1)
      case "2G" => list :+= ("D00020004 ", 1)
      case _ => list :+= ("D00020005 ", 1)
    }

    isp match  {
      case "移动" => list :+= ("D00030001", 1)
      case "联通" => list :+= ("D00030002", 1)
      case "电信" => list :+= ("D00030003", 1)
      case _ => list :+= ("D00030004", 1)
    }

    list
  }
}
