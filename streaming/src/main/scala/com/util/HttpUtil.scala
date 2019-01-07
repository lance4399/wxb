package com.util

import com.alibaba.fastjson.JSON

import scala.util.Try

/**
  * @Author: lance
  * @Date: 2018/12/1
  */
object HttpUtil {

  def get(url: String,
          connectTimeout: Int = 5000,
          readTimeout: Int = 5000,
          requestMethod: String = "GET"): String = {
    import java.net.{HttpURLConnection, URL}
    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    val inputStream = connection.getInputStream
    val content = scala.io.Source.fromInputStream(inputStream,"utf-8").mkString

    return content

  }

  def sendGet(url: String): String = {
    try {
      val content = get(url)
      return content
    } catch {
      case ioe: java.io.IOException => ioe.printStackTrace()
      case ste: java.net.SocketTimeoutException => ste.printStackTrace()
    }
    ""
  }

  def main(args: Array[String]): Unit = {

    val array = Seq("114246456","111878597")
    val jedisClusterUtil = new RedisClusterUtil
    var contentIds:String = new String
    for(i <- array){
      contentIds = contentIds +i +","
    }

    contentIds="xxxx"
    contentIds = contentIds.substring(0,contentIds.length - 1)
    val url = s"http://mp.data.sohuno.com/stat/news/${contentIds}/totalpv"
    val apiResult = Try( JSON.parseArray(HttpUtil.sendGet(url)) ).get
    if(null != apiResult){
      println(s"apiResult=${apiResult}")
    }else{
      println(s"apiResult is null =${apiResult}")
    }
      println("finally")

  }

}

