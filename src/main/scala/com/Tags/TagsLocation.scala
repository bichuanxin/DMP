package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/23
  */
object TagsLocation extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")
    if (!provincename.equals("未知")){
      list :+= ("ZP"+ provincename, 1)
    }
    if (! cityname.equals("未知")){
      list :+= ("ZC" + cityname, 1)
    }
    list
  }
}
