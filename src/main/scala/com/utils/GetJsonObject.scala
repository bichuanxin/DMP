package com.utils

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/20
  */
object GetJsonObject {

  def getVlaueByField(json: String, field: String) : String = {
    try{
      val jsonObject: JSONObject = JSON.parseObject(json)
      val value = jsonObject.getString(field)
      value
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }
  }

}
