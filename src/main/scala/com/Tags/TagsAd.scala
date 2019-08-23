package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * @note 广告标签
  * @Author Bi ChuanXin
  * @Date 2019/8/23
  */
object TagsAd extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    val adType = row.getAs[Int]("adspacetype")
    adType match {
      case v if v>9 => list:+=("LC"+v, 1)
      case v if v<=9&&v>0 => list:+=("LC0"+v, 1)
    }

    val adname = row.getAs[String]("adspacetypename")
    if (StringUtils.isNoneBlank(adname)){
      list :+= ("LN"+adname, 1)
    }
    list
  }
}
