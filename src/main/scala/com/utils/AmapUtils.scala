package com.utils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable

/**
  * @note 商圈解析工具
  * @Author Bi ChuanXin
  * @Date 2019/8/24
  */
object AmapUtils {
  //获取高德地图商圈信息

  def getBusinessFromAmap(long: Double, lat: Double) :String = {
//    https://restapi.amap.com/v3/geocode/regeo?parameters
    val url = "https://restapi.amap.com/v3/geocode/regeo" +
      "?location="+long+","+lat+"&key=797068247ffa14df698ce52c659e5282"
    val jsonstr = HTTPUtils.get(url)
    val jsonObject = JSON.parseObject(jsonstr)
    //判断状态是否成功
    val status = jsonObject.getIntValue("status")
    if (status == 0) return ""

    //解析内部json,判断每个key的value都不能为空
    val regeocodeJson = jsonObject.getJSONObject("regeocode")
    if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""
    val addrJson = regeocodeJson.getJSONObject("addressComponent")
    if (addrJson == null || addrJson.keySet().isEmpty) return ""
    val businessJsonArr = addrJson.getJSONArray("businessAreas")
    if (businessJsonArr == null ) return ""
    val buffer = mutable.ListBuffer[String]()
    val arr: JSONArray = businessJsonArr

    for (elem <- businessJsonArr.toArray()){
      if (elem.isInstanceOf[JSONObject]){
        val json = elem.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }
    buffer.mkString(",")


  }

//  def main(args: Array[String]): Unit = {
//    println(getBusinessFromAmap(116.310003, 39.991957))
//  }

}
