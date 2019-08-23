package com.ETL

import com.utils.DBProperties
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/21
  */
object GetAppDict {
  def main(args: Array[String]): Unit = {
    val conf  = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //    val Array(inputPath) = args
    val dicts: RDD[appDict] = sc.textFile("C:\\Users\\sora\\Desktop\\Spark用户画像分析\\app_dict.txt")
      .map(x => {
        val fields = x.split("\t")
        var appName: String = null
        var appId: String = null
        try{
          appName = fields(1).trim
          appId = fields(4).trim
        }catch{
            case e:Exception => {
            appName = ""
            appId = ""
          }
        }

        appDict(appId, appName)
      }).filter(_.appid.length != 0)
    val prop = DBProperties.getDBConfig()

    sQLContext.createDataFrame(dicts).write
        .mode(SaveMode.Overwrite)
        .jdbc(prop.getProperty("url"), "appdict", prop)
    sc.stop()

  }

}
case class appDict(appid: String, appname: String)