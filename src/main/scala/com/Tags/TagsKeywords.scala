package com.Tags

import com.utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/23
  */
object TagsKeywords extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]
    val stopword = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]]

    val keywords = row.getAs[String]("keywords")

    if (keywords.equals("") || keywords.length == 0){
      list
    }else{
      val words = keywords.split("\\|")
      for (word <- words){
        if (word.length >= 3 && word.length <= 8 && !stopword.value.contains(word) ){
          list :+= ("K"+word, 1)
        }
      }
      list
    }
  }
}
