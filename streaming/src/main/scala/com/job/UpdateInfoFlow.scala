package com.job
import com.alibaba.fastjson.JSON
import com.util.{Constants, HttpUtil, KafkaSink, RedisClusterUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.util.Try

/**
  * @Author: lance
  * @Date: 2018/12/3
  */
object UpdateInfoFlow {

  object UpdateFlow_Conf {
    val appName = "wxb_update_info_batch"
    val duration = 10
    val sparkConf = new SparkConf().setAppName(appName).set("spark.speculation", "false")
      .set("spark.streaming.kafka.maxRatePerPartition", "10000")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.scheduler.executorTaskBlacklistTime", "30000")
      .set("spark.network.timeout", "360000")
      .set("spark.streaming.kafka.consumer.poll.ms", "4096")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.rdd.compress", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(duration))
    val sc = ssc.sparkContext
    val appId = sc.applicationId

  }

  def main(args: Array[String]): Unit = {

    val topics = Array(Constants.topic_pv_dwd, Constants.topic_ev_dwd)
    val kafkaParams = Constants.cdh_kafka_cluster
    val stream = KafkaUtils.createDirectStream[String, String](
      UpdateFlow_Conf.ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    val kafkaProducer = UpdateFlow_Conf.sc.broadcast(KafkaSink[String, String](Constants. cdh_kafka_cluster))
    val redisClient = UpdateFlow_Conf.sc.broadcast(new RedisClusterUtil) // broadcast redis

    stream.filter(_ != null ).
      map(consumerRecord => Try(JSON.parseObject(consumerRecord.value()).getString("contentId"))
        .getOrElse("")).filter(!_.equals("")).filter(!_.equals("-"))
      .foreachRDD(rdd => { //driver
        rdd.filter(_ != null)
          .distinct(20)
          .foreachPartition(p => { //executor
            val producer = kafkaProducer.value
            val jedisClusterUtil = redisClient.value
            var contentIds = new String
            var index: Int = 0
            while (p.hasNext) {
              val contentId = p.next()
              val combinationKey = s"wxb:article:add:${contentId}"
              if (!"-".equals(contentId) && jedisClusterUtil.exists(combinationKey)) {
                contentIds = contentIds + contentId + ","
                index += 1
              }

              if (index == 20 ) {
                println("记录等于20条时->:")
                contentIds = contentIds.substring(0, contentIds.length - 1)
                val url = s"http://apiDemo.data.com/stat/news/${contentIds}/totalpv"
                val apiResult = Try(JSON.parseArray(HttpUtil.sendGet(url))).get
                if (null != apiResult) {
                  for (i <- 0 until apiResult.size()) { //the travelsal of api result(20 times)
                    val each = apiResult.getJSONObject(i)
                    val entry = each.entrySet() //the keySet of each article:newsId,totalpcPV,totalpcEv,and so on
                    import scala.collection.JavaConversions._
                    val res = entry.filter(_.getKey.matches("totalPcPv|totalPcEv|totalWapPv|totalWapEv|totalAppPv|totalAppEv"))
                      .map(v => Try(v.getValue.toString.toLong).getOrElse(0l))
                      .reduce(_+_)

                    println(s"第${i + 1}篇文章所返回的数据:${each.toJSONString}")
                    if (res > 100) { // reach to the updating threshold
                      /** 1.send the update info **/
                      val articleId = each.getString("newsId")
                      val updateArticleId = s"wxb:article:update:${articleId}"

                      //try{
                      val to_update_article_info = jedisClusterUtil.get(updateArticleId)
                      producer.send(Constants.topic_to_wxb, to_update_article_info)
                      println(s"向kafka发送了文章更新消息->第${index}篇文章id为:${articleId},其pvev=${res},内容为:${to_update_article_info}")
                      //}
                      //catch{
                      //  case e:Exception => println(e)
                      //}
                      val members = jedisClusterUtil.smembers(combinationKey)
                      if(members.size() > 0){
                        for (member <- members) {
                          if(!"".equals(member)){
                            producer.send(Constants.topic_to_wxb, member)
                            println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
                            println(s"向kafka发送了评论更新消息,评论更新信息->${member}")
                          }
                        }
                      }

                      /** 2.delete from redis **/
                      //                      println(each.getString("newsId") + s"##### redis del invoked,pvev=${res} ########")
                      //                      println(each.getString("newsId") + "update  " + s"##### redis del invoked,pvev=${res} ########")
                      jedisClusterUtil.del( combinationKey  )
                      jedisClusterUtil.del( updateArticleId )
                    }
                  }
                }
                index = 0
                contentIds = new String
              }

            }
            println("#######################")
            println("剩余记录不满20条时:")
            if( contentIds.length > 1){
              contentIds = contentIds.substring(0, contentIds.length - 1) //此时不满20条记录
              val url = s"http://apiDemo.data.com/stat/news/${contentIds}/totalpv"
              val apiResult = Try(JSON.parseArray(HttpUtil.sendGet(url))).get
              if (null != apiResult) {
                for (i <- 0 until apiResult.size()) { //the travelsal of api result(20 times)
                  val each = apiResult.getJSONObject(i)
                  val entry = each.entrySet() //the keySet of each article:newsId,totalpcPV,totalpcEv,and so on
                  import scala.collection.JavaConversions._
                  val res = entry.filter(_.getKey.matches("totalPcPv|totalPcEv|totalWapPv|totalWapEv|totalAppPv|totalAppEv"))
                    .map(v => Try(v.getValue.toString.toLong).getOrElse(0l))
                    .reduce(_+_)


                  println(s"第${i + 1}篇文章所返回的数据:${each.toJSONString}")
                  if (res > 100) { // reach to the updating threshold
                    /** 1.send the update info **/
                    val articleId = each.getString("newsId")
                    val updateComment_ArticleId = s"wxb:article:add:${articleId}"
                    val updateArticleId = s"wxb:article:update:${articleId}"
                    //                    try{
                    val to_update_article_info = jedisClusterUtil.get(updateArticleId)
                    producer.send(Constants.topic_to_wxb, to_update_article_info)
                    println(s"向kafka发送了文章更新消息->第${index}篇文章id为:${articleId},其pvev=${res},内容为:${to_update_article_info}")
                    //                    }
                    //                    catch{
                    //                      case e:Exception => println(e)
                    //                    }

                    val members = jedisClusterUtil.smembers(updateComment_ArticleId)
                    for (member <- members) {
                      if(!"".equals(member)){
                        producer.send(Constants.topic_to_wxb, member)
                        println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
                        println(s"向kafka发送了评论更新消息,评论更新信息->${member}")
                      }
                    }

                    /** 2.delete from redis **/
                    jedisClusterUtil.del( updateComment_ArticleId )
                    jedisClusterUtil.del( updateArticleId  )
                  }
                }
              }
              index = 0
              contentIds = new String
            }

          })
      })

    UpdateFlow_Conf.ssc.start()
    UpdateFlow_Conf.ssc.awaitTermination()

  }

}
