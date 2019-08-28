package com.week1

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ListBuffer

/**
  * @note 处理json工具类
  * @Author Bi ChuanXin
  * @Date 2019/8/24
  */
object JsonObjectUtils {

  // 获取pois json数组
  def getPoisJsonArr(json: String) :JSONArray  = {
    //初始化json对象
    val jsonObject: JSONObject = JSON.parseObject(json)

    //判断json状态位
    val status = jsonObject.getIntValue("status")
    if (status == 0) return null

    //获取pois json对象
    //获取regeocode json对象并判断是否为空
    val regeocodeJson = jsonObject.getJSONObject("regeocode")
    if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return null
    //获取pois json数组
    val poisjsonArr: JSONArray = regeocodeJson.getJSONArray("pois")
    if (poisjsonArr == null) return null
    poisjsonArr
  }

  //获取商圈
  def getBusinessFromPois(json: String) :String = {
    //初始化list
    var list = ListBuffer[String]()
    //获取pois json数组
    val poisArr: JSONArray = getPoisJsonArr(json)
    if (poisArr == null) return ""

    //遍历数组取出商圈
    for (item <- poisArr.toArray) {
      if (item.isInstanceOf[JSONObject]) {
        val itemJson = item.asInstanceOf[JSONObject]
        val ba = itemJson.getString("businessarea")

        if (!ba.equals("[]") && StringUtils.isNoneBlank(ba) && ba.length != 0){
          list.append(ba)
        }

      }
    }
    list.mkString(",")
  }

  //获取商圈类型
  def getBusinessTypeFromPois(json: String) :String = {
    //初始化list
    var list = ListBuffer[String]()
    //获取pois json数组
    val poisArr: JSONArray = getPoisJsonArr(json)
    if (poisArr == null) return ""

    //遍历数组取出商圈类型
    for (item <- poisArr.toArray) {
      if (item.isInstanceOf[JSONObject]) {
        val itemJson = item.asInstanceOf[JSONObject]
        val types = itemJson.getString("type")
        val typearr = types.split(";")
        typearr.foreach(x => list.append(x))
      }
    }
    list.mkString(",")
  }

}
