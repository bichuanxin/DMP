package com.utils

/**
  * @note 指标方法
  * @Author Bi ChuanXin
  * @Date 2019/8/21
  */
object RptUtils {
  //处理请求数
  def request(mode: Int, node: Int):List[Double] = {
    if (mode == 1){
      node match {
        case 3 => List(1.0, 1.0, 1.0)
        case 2 => List(1.0, 1.0, 0.0)
        case 1 => List(1.0, 0.0, 0.0)
      }
    }else{
      List(0, 0, 0)
    }

  }

  //处理展示点击数
  def click(mode: Int, ef: Int) :List[Double] = {
    if (ef == 1){
      mode match {
        case 2 => List(1, 0)
        case 3 => List(0, 1)
      }
    }else{
      List(0, 0)
    }

  }

  //处理竞价操作数
  def compete(ef: Int, billing: Int, bid: Int, win: Int,
              adorderid: Int, winPrice: Double,
              adpayment: Double) :List[Double] = {
    if (ef == 1 && billing == 1){
        bid match {
          case 1 => List(1, 0, 0, 0)
          case _ => win match {
            case 1 => adorderid match {
              case 0 => List(0, 0, winPrice / 1000, adpayment / 1000)
              case _ => List(0, 1, winPrice / 1000, adpayment / 1000)
            }
            case _ => List(0, 0, 0, 0)
          }
        }
    }else{
      List(0, 0, 0, 0)
    }
  }
  //终端校验
  def checkTerminal(name: String) :String = {
    name match {
      case "移动" => "移动"
      case "联通" => "联通"
      case "电信" => "电信"
      case _ => "其他"
    }
  }
  //网络校验
  def checkNetType(netType: String) : String = {
    netType match {
      case "2G" => "2G"
      case "3G" => "3G"
      case "4G" => "4G"
      case "Wifi" => "Wifi"
      case _ => "其他"
    }
  }
  //校验设备
  def checkDeciceType(devicetype: Int) :String = {
    devicetype match {
      case 1 => "手机"
      case 2 => "平板"
      case _ => "其他"
    }
  }
  //校验OS
  def checkOS(os: Int) :String = {
    os match {
      case 1 => "Android"
      case 2 => "IOS"
      case _ => "其他"
    }
  }




}
