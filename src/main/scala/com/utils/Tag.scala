package com.utils

/**
  * @note
  * @Author Bi ChuanXin
  * @Date 2019/8/23
  */
trait Tag {
  /**
    * 打标签的统一接口
    */
  def makeTags(args:Any*) :List[(String, Int)]

}
