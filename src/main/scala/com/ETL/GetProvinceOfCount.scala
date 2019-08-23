package com.ETL

import java.util.Properties

import com.utils.{DBProperties, GetJsonObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/20
  */
object GetProvinceOfCount {
  def main(args: Array[String]): Unit = {
    //匹配参数个数
    if (args.length != 2){
      println("参数个数不匹配")
      sys.exit()
    }
    //初始化sc和sql
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //获取数据库配置信息
    val prop = DBProperties.getDBConfig()
    //调用sql
    getCountsBySQL(sqlContext, args, prop)
    sc.stop()

  }
  def getCountByCore(sc: SparkContext, args: Array[String]): Unit ={
    //获取路径
    val Array(inputPath, outputPath) = args
    val lines: RDD[String] = sc.textFile(inputPath)

    val tupOflogs = lines.map(line => {
      ((GetJsonObject.getVlaueByField(line,"provincename"), GetJsonObject.getVlaueByField(line,"cityname")), 1)
    }).reduceByKey(_ + _).map(x => (x._1._1, x._1._2, x._2))

    tupOflogs.saveAsTextFile(outputPath)

  }

  def getCountsBySQL(sqlContext: SQLContext, args: Array[String], prop: Properties): Unit ={
    //获取输入路径
    val Array(inputPath, outputPath) = args
    //读取parquet格式文件
    val logDF: DataFrame = sqlContext.read.parquet(inputPath)
    //注册临时表logs
    logDF.registerTempTable("logs")
    //sql查询
    val ctDF = sqlContext.sql("select count(*) as ct, provincename, cityname " +
      "from logs group by provincename, cityname")

    //输出json格式到hdfs并按字段分区
    logDF.write.mode(SaveMode.Append).json("D:\\outputFiles\\output-2019-08-20-json")
//    ctDF.write.mode(SaveMode.Append)
//      .format("json")
//      .partitionBy("provincename","cityname")
//      .save(outputPath)
//    //输出到mysql中
//    ctDF.write.mode(SaveMode.Overwrite)
//      .option("createTableColumnTypes","ct int, provincename varchar(50), cityname varchar(50)")
//      .jdbc(prop.getProperty("url"), "province_ct", prop)
  }
}
