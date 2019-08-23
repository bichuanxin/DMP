package com.utils

/**
  * @note 数据类型转换
  * @Author Bi ChuanXin
  * @Date 2019/8/20
  */
object Utils2Type {
  //String转换int
  def toInt(str: String) : Int = {
    try {
      str.toInt
    }catch {
      case  _:Exception => 0
    }

  }

  //String转换double
  def toDouble(str: String) : Double = {
    try {
      str.toDouble
    }catch {
      case  _:Exception => 0
    }

  }

}
