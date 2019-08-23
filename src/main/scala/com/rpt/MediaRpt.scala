package com.rpt

import com.utils.{AppDictMap, DBProperties, RptUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/21
  */
object MediaRpt {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    //判断路径个数
    if (args.length != 2){
      println("参数个数不匹配")
      sys.exit()
    }
    //获取路径
    val Array(inputPath, outputPath) = args
    val sQLContext = new SQLContext(sc)

    val df = sQLContext.read.parquet(inputPath)

    val prop = DBProperties.getDBConfig()
    val dictDf = sQLContext.read.jdbc(prop.getProperty("url"), "appdict", prop)
    df.registerTempTable("log")
    dictDf.registerTempTable("appdict")

    val res = sQLContext.sql("select adappname, sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as amt_all_rpt, sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as amt_eff_rpt, sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) as amt_adr_rpt, sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as amt_display, sum(case when requestmode =3 and iseffective = 1 then 1 else 0 end) as amt_click, sum(case when iseffective = 1 and isbilling = 1 then 1 else 0 end) as amt_in_compete, sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid <> 0 then 1 else 0 end) as amt_win_compete, sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice / 100 else 0 end) as amt_100perWinprice, sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment / 100 else 0 end) as amt_100perAdpay from( select  lo.*, ad.appname as adappname from log lo left join appdict ad on lo.appid = ad.appid where ad.appname is not null ) a group by adappname")
    res.show()
    val dict = sc.broadcast(AppDictMap.getAppDictMap(sQLContext))

    val res1 = df.map(row => {
      //把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorerid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      val appid = row.getAs[String]("appid")
      var appname = row.getAs[String]("appname")
      if (appname.equals("其他")){
        appname = dict.value.getOrElse(appid, "None")
      }

      val list = RptUtils.request(requestmode, processnode)
        .union(RptUtils.click(requestmode, iseffective))
        .union(RptUtils.compete(iseffective, isbilling, isbid, iswin, adorerid, winprice, adpayment))

      (appname, list)
    }).reduceByKey((l1, l2) => {
      l1.zip(l2).map(tup => {
        tup._1 + tup._2
      })
    }).map(x => {
      (x._1, x._2(0).toInt, x._2(1).toInt, x._2(2).toInt, x._2(3).toInt,
        x._2(4).toInt, x._2(5).toInt, x._2(6).toInt, x._2(7), x._2(8))
    }).filter(x => !x._1.equals("None"))
    res1.collect.foreach(println)
    sc.stop()

  }
}
