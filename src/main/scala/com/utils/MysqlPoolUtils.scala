package com.utils
import java.io.FileInputStream
import java.sql.Connection
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import org.apache.log4j.Logger
import javax.sql.DataSource

/**
  * @author yangxin_ryan
  * Mysql数据库连接池
  */
object MysqlPoolUtils {

  private val LOG = Logger.getLogger(MysqlPoolUtils.getClass.getName)

  val dataSource: Option[DataSource] = {
    try {
      val druidProps = new Properties()
      // 获取Druid连接池的配置文件

      // 倒入配置文件
      druidProps.load(new FileInputStream("D:\\GIT\\GP_22_DMP\\src\\main\\resources\\application.properties"))
      Some(DruidDataSourceFactory.createDataSource(druidProps))
    } catch {
      case error: Exception =>
        LOG.error("Error Create Mysql Connection", error)
        None
    }
  }

  // 连接方式
  def getConnection: Option[Connection] = {
    dataSource match {
      case Some(ds) => Some(ds.getConnection())
      case None => None
    }
  }
}
