package com.rpt

import java.sql.PreparedStatement

import com.utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.utils.MysqlPoolUtils

/**
  * @note 地域分布指标
  * @Author Bi ChuanXin
  * @Date 2019/8/21
  */
object LocationRpt {
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

//    df.registerTempTable("log")
//    val res  = sQLContext.sql("select  provincename, cityname, sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as amt_all_rpt, sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as amt_eff_rpt, sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) as amt_adr_rpt, sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as amt_display, sum(case when requestmode =3 and iseffective = 1 then 1 else 0 end) as amt_click, sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) as amt_in_compete, sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid <> 0 then 1 else 0 end) as amt_win_compete, sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice / 100 else 0 end) as amt_100perWinprice, sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment / 100 else 0 end) as amt_100perAdpay from log group by  provincename, cityname")
//    res.show()

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

      val provincename = row.getAs[String]("provincename")
      val cityname = row.getAs[String]("cityname")

      val list = RptUtils.request(requestmode, processnode)
        .union(RptUtils.click(requestmode, iseffective))
        .union(RptUtils.compete(iseffective, isbilling, isbid, iswin, adorerid, winprice, adpayment))

      ((provincename, cityname), list)
    }).reduceByKey((l1, l2) => {
      l1.zip(l2).map(tup => {
        tup._1 + tup._2
      })
    }).map(x => {
      (x._1._1, x._1._2, x._2(0).toInt, x._2(1).toInt, x._2(2).toInt, x._2(3).toInt,
        x._2(4).toInt, x._2(5).toInt, x._2(6).toInt, x._2(7), x._2(8))
    })
    res1.foreachPartition(x=>{
      val conn = MysqlPoolUtils.getConnection.get
      x.foreach(x => {
        val sql = "INSERT INTO locationrpt " +
          "(`province`, `cityname`, `amt_all_rpt`, `amt_eff_rpt`, `amt_adr_rpt`, " +
          "`amt_display`, `amt_click`, `amt_in_compete`, `amt_win_compete`, " +
          "`amt_100perWinprice`, `amt_100perAdpay`) " +
          "VALUES('" + x._1 + "', '" + x._2 + "', '" + x._3 + "', '" + x._4 + "', " +
          "'" + x._5 + "', '" + x._6 + "', '" + x._7 + "', '" + x._8 + "', '" + x._9 + "', " +
          "'" + x._10 + "', '" + x._11 + "')"
//        val conn = MysqlPoolUtils.getConnection.get
        val statement: PreparedStatement = conn.prepareStatement(sql)
        statement.execute()
//        conn.close()
      })
      conn.close()
    })
    sc.stop()

  }

}
