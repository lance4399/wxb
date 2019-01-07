package com.util

import java.util.Date

import com.alibaba.fastjson.{JSONArray, JSONObject}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

/**
  * @Author: lance
  * @Date: 2018/12/1
  */
object Constants {

  val topic_pv_dwd = "topic_pv_dwd"
  val topic_ev_dwd = "topic_ev_dwd"
  val topic_test = "topic_test"

  val topic_to_wxb = "topic_to_wxb"

  val rtp_base_url = "http://data-ap.com/news/"

  val date = new Date().getTime.toString

  val cdh_kafka_cluster = Map[String, Object](
    "bootstrap.servers" ->   """
                             xx
                             """,
    "key.serializer" -> classOf[StringSerializer],
    "value.serializer" -> classOf[StringSerializer] ,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> s"wxb_fetch_datasource_${date}" ,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )


  val msg_version_str = "msg_version"
  val msg_version_1_0 = "v1.0"
  val msg_version_1_1 = "v1.1"
  val msg_type_str = "msg_type"
  val total_pushed_num = "total_pushed_num"
  val record_list = "record_list"
  val selfmedia_article = "article"
  val selfmedia_articleupd = "articleupd"
  val selfmedia_comment = "comment"
  val selfmedia_commentupd = "commentupd"
  val selfmedia_author = "author"
  val selfmedia_authorupd = "authorupd"
  val selfmedia_hashuidupd = "hashuidupd"



  def formatResult(msg_version:Int,msg_type:Int,records:JSONArray):JSONObject = {
    val res = new JSONObject()
    res.put(msg_version_str,msg_version)
    res.put(msg_type_str,msg_type)
    res.put(total_pushed_num,records.size())
    res.put(record_list,records)
    res
  }

}
