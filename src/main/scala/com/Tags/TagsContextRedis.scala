package com.Tags

import com.typesafe.config.ConfigFactory
import com.utils.{AppDictMap, RedisPool, TagsUtils}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * @note 上下文标签
  * @Author Bi ChuanXin
  * @Date 2019/8/23
  */
object TagsContextRedis {
  def main(args: Array[String]): Unit = {
    if (args.length != 5){
      println("目录不匹配")
      sys.exit()
    }

    val Array(inputPath, outputPath,a ,stopPath, day) = args
    //创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //调用hbase API
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.TableName")
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum" , load.getString("hbase.host"))

    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin

    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }
    //创建jobconf
    val jobconf = new JobConf(configuration)
    // 指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)





    val stopWord = sc.textFile(stopPath).map((_,0)).collectAsMap()
    val stopBrod = sc.broadcast(stopWord)


    //读取数据
    val df = sQLContext.read.parquet(inputPath)

//    AppDictMap.initRedis(sQLContext)
    // 过滤符合id的数据
    val tag = df.filter(TagsUtils.OneUserId)
      .mapPartitions(part =>{
        val conn: Jedis = RedisPool.getConnection()

        val tuples: Iterator[(String, List[(String, Int)])] = part.map(row => {
          //取出用户id
          val userid = TagsUtils.getOneUserId(row)
          // 接下来通过row数据 打上 所有标签（按照需求）
          //广告类型标签
          val adTags = TagsAd.makeTags(row)
          //app标签
          val appTags = TagsAppRedis.makeTags(row, conn)
          //渠道标签
          val channelTags = TagsChannel.makeTags(row)
          //设备标签
          val deviceTags = TagsDevice.makeTags(row)
          //关键字标签
          val keywordsTags = TagsKeywords.makeTags(row, stopBrod)
          // 地域标签
          val locationTags = TagsLocation.makeTags(row)
          // 商圈标签
          val businessTags = BusinessTag.makeTags(row)
          //整合
          val tags: List[(String, Int)] = adTags.union(appTags)
            .union(channelTags)
            .union(deviceTags)
            .union(keywordsTags)
            .union(locationTags)
            .union(businessTags)
          (userid, tags)
        })
        conn.close()
        tuples
      })
    // 聚合
    tag.reduceByKey((l1, l2) => {
      val list: List[(String, Int)] = l1.union(l2)
      val grouped: Map[String, List[(String, Int)]] = list.groupBy(_._1)
      val sum: Map[String, Int] = grouped.mapValues(_.size)
      val value: List[(String, Int)] = sum.toList
      value
    }).map{
      case (userid, userTags) => {
        val put = new Put(Bytes.toBytes(userid))
        //处理标签
        val tags = userTags.map(t => t._1+","+t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(day), Bytes.toBytes(tags))
        (new ImmutableBytesWritable(), put)


      }
    }.saveAsHadoopDataset(jobconf)
//    tag1.collect.foreach(println)
  }
}
