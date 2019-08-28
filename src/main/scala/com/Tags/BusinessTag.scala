package com.Tags

import ch.hsr.geohash.GeoHash
import com.utils.{AmapUtils, RedisPool, Tag, Utils2Type}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * @note 商圈标签
  * @Author Bi ChuanXin
  * @Date 2019/8/26
  */
object BusinessTag extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]
    //获取经纬度,过滤经纬度
    val longDou = Utils2Type.toDouble(row.getAs[String]("long"))
    val latDou = Utils2Type.toDouble(row.getAs[String]("lat"))

    if (longDou >= 73 && longDou <=135 && latDou >= 3 && latDou <= 54){
      // 先获取数据的商圈
      val business = getBusiness(longDou, latDou)

      if (StringUtils.isNotBlank(business)){
        val businesses = business.split(",")
        businesses.foreach(x => list :+= (x, 1))
      }
    }
    list
  }

  /**
    * 获取商圈信息
    */
  def getBusiness(long: Double, lat: Double) :String ={
    //转换geohash字符串
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 8)
    // 取数据库查询
    var business = redisQueryBusiness(geohash)
    // 判断是否为空
    if (business == null || business.length ==0 ){
      //通过经纬度获取商圈
      business = AmapUtils.getBusinessFromAmap(long, lat)
      redisInsertBusiness(geohash, business)
    }
    business

  }

  /**
    * 获取商圈信息数据库
    */
  def redisQueryBusiness(geoHash: String) :String = {
    val jedis = RedisPool.getConnection()
    val business = jedis.get(geoHash)
    jedis.close()
    business
  }

  /**
    * 存储商圈redis
    */
  def redisInsertBusiness(geohash: String , business: String): Unit ={
    val jedis = RedisPool.getConnection()
    jedis.set(geohash,business)
    jedis.close()
  }
}
