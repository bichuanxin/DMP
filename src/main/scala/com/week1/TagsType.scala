package com.week1

import com.utils.Tag
import org.apache.commons.lang3.StringUtils

/**
  * @note 处理type标签
  * @Author Bi ChuanXin
  * @Date 2019/8/24
  */
object TagsType extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //转换参数
    val json = args(0).asInstanceOf[String]
    val typeStr = JsonObjectUtils.getBusinessTypeFromPois(json)
    if (StringUtils.isNotBlank(typeStr)){
      val types = typeStr.split(",")
      types.foreach(x => list :+= ("BT"+ x, 1))
    }
    list
  }
}
