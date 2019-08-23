package com.utils

import java.io.FileReader
import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/23
  */
object RedisPool {

  private var jedisSource: JedisPool = null

  //初始化连接池
  def getJedisSource() :JedisPool ={
    val prop = new Properties
    prop.load(new FileReader("D:\\GIT\\GP_22_DMP\\src\\main\\resources\\DBConfig.properties"))

    val jconf = new JedisPoolConfig()
    jconf.setMaxTotal(prop.getProperty("redis.MaxTotal").toInt)
    jconf.setMaxIdle(prop.getProperty("redis.MaxIdle").toInt)
    jconf.setMaxWaitMillis(prop.getProperty("redis.timeout").toLong)

    val pool = new JedisPool(jconf,prop.getProperty("redis.host"),prop.getProperty("redis.port").toInt)
    pool
  }

  //获取连接对象
  def getConnection() :Jedis ={
    if (jedisSource == null){
      jedisSource = getJedisSource()
    }
    jedisSource.getResource
  }

}
