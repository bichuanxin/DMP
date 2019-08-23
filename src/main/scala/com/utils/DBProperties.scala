package com.utils

import java.io.FileReader
import java.util.Properties

/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/20
  */
object DBProperties {
  /**
    *
    * @return 返回数据库配置信息
    */
  def getDBConfig():Properties = {
    val prop = new Properties()
    prop.load(new FileReader("src/main/resources/DBConfig.properties"))
    return prop
  }
}
