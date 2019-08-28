package com.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * @note 标签工具类
  * @Author Bi ChuanXin
  * @Date 2019/8/23
  */
object TagsUtils {
   //过滤需要的字段
  val OneUserId=
    """
      | imei != '' or mac != '' or openudid != '' or androidid != '' or idfa != '' or
      | imeimd5 != '' or macmd5 != '' or openudidmd5 != '' or androididmd5 != '' or idfamd5 != '' or
      | imeisha1 != '' or macsha1 != '' or openudidsha1 != '' or androididsha1 != '' or idfasha1 != ''
    """.stripMargin

  //取出唯一不为空的id
  def getOneUserId(row: Row) :String = {
    row match {
      case v if StringUtils.isNoneBlank(v.getAs[String]("imei")) => "IM: " + v.getAs[String]("imei")
      case v if StringUtils.isNoneBlank(v.getAs[String]("mac")) => "MAC: " + v.getAs[String]("mac")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudid")) => "OP: " + v.getAs[String]("openudid")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androidid")) => "AN: " + v.getAs[String]("androidid")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfa")) => "ID: " + v.getAs[String]("idfa")
      case v if StringUtils.isNoneBlank(v.getAs[String]("imeimd5")) => "IM5: " + v.getAs[String]("imeimd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("macmd5")) => "MAC5: " + v.getAs[String]("macmd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudidmd5")) => "OP5: " + v.getAs[String]("openudidmd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androididmd5")) => "AN5: " + v.getAs[String]("androididmd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfamd5")) => "ID5: " + v.getAs[String]("idfamd5")
      case v if StringUtils.isNoneBlank(v.getAs[String]("imeisha1")) => "IM1: " + v.getAs[String]("imeisha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("macsha1")) => "MAC1: " + v.getAs[String]("macsha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudidsha1")) => "OP1: " + v.getAs[String]("openudidsha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androididsha1")) => "AN1: " + v.getAs[String]("androididsha1")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfasha1")) => "ID1: " + v.getAs[String]("idfasha1")
    }
  }
  //获取所有id
  def getAllUserId(row: Row):List[String] = {
    var list = List[String]()
    if (StringUtils.isNoneBlank(row.getAs[String]("imei")) ) list:+= "IM: " + row.getAs[String]("imei")
    if (StringUtils.isNoneBlank(row.getAs[String]("mac")) ) list:+= "MAC: " + row.getAs[String]("mac")
    if (StringUtils.isNoneBlank(row.getAs[String]("openudid")))  list:+= "OP: " + row.getAs[String]("openudid")
    if (StringUtils.isNoneBlank(row.getAs[String]("androidid")))  list:+= "AN: " + row.getAs[String]("androidid")
    if (StringUtils.isNoneBlank(row.getAs[String]("idfa")))  list:+= "ID: " + row.getAs[String]("idfa")
    if (StringUtils.isNoneBlank(row.getAs[String]("imeimd5")))  list:+= "IM5: " + row.getAs[String]("imeimd5")
    if (StringUtils.isNoneBlank(row.getAs[String]("macmd5")))  list:+= "MAC5: " + row.getAs[String]("macmd5")
    if (StringUtils.isNoneBlank(row.getAs[String]("openudidmd5")))  list:+= "OP5: " + row.getAs[String]("openudidmd5")
    if (StringUtils.isNoneBlank(row.getAs[String]("androididmd5")))  list:+= "AN5: " + row.getAs[String]("androididmd5")
    if (StringUtils.isNoneBlank(row.getAs[String]("idfamd5")))  list:+= "ID5: " + row.getAs[String]("idfamd5")
    if (StringUtils.isNoneBlank(row.getAs[String]("imeisha1")))  list:+= "IM1: " + row.getAs[String]("imeisha1")
    if (StringUtils.isNoneBlank(row.getAs[String]("macsha1")))  list:+= "MAC1: " + row.getAs[String]("macsha1")
    if (StringUtils.isNoneBlank(row.getAs[String]("openudidsha1")))  list:+= "OP1: " + row.getAs[String]("openudidsha1")
    if (StringUtils.isNoneBlank(row.getAs[String]("androididsha1")))  list:+= "AN1: " + row.getAs[String]("androididsha1")
    if (StringUtils.isNoneBlank(row.getAs[String]("idfasha1")))  list:+= "ID1: " + row.getAs[String]("idfasha1")

    list

  }
}
