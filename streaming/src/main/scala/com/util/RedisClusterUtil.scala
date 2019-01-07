package com.util

import java.util.Date

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{HostAndPort, JedisCluster}

import scala.collection.JavaConverters._

/**
  * @Author: lance
  * @Date: 2018/12/1
  */
class RedisClusterUtil extends Serializable {

  lazy val redisClient = init()

  val exTime = 48*60*60 //2天

  sys.addShutdownHook(
    {println(s"redis client shutdown -- ${new Date().getTime}")
      redisClient.close()}
  )

  def init() :JedisCluster= {
    println(s"redis client init -- ${new Date().getTime}")
    val hostAndPortsSet = Set(
      new HostAndPort("node1",201701),
      new HostAndPort("node2",201702),
      new HostAndPort("node3",201703)

    )
    val jedisPoolConfig = new GenericObjectPoolConfig
    // 最大空闲连接数, 默认8个// 最大空闲连接数, 默认8个
    jedisPoolConfig.setMaxIdle(10)
    // 最大连接数, 默认8个
    jedisPoolConfig.setMaxTotal(50)
    //最小空闲连接数, 默认0
    jedisPoolConfig.setMinIdle(1)
    // 获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
    jedisPoolConfig.setMaxWaitMillis(2000) // 设置2秒
    //对拿到的connection进行validateObject校验
    jedisPoolConfig.setTestOnBorrow(true)
    //    new JedisPool(jedisPoolConfig,"10.18.4.87",19010,10000,"ecF84K7e")
    new JedisCluster(hostAndPortsSet.asJava , 2000 , 1000 , 5 , "password", jedisPoolConfig)
  }

  def sadd(key:String, value:String) = {
    val res = redisClient.sadd(key,value)
    //    redisClient.close()
    res
  }


  def smembers(key:String) = {
    val res = redisClient.smembers(key)
    //    redisClient.close()
    res
  }

  def exists(key:String) ={
    val flag = redisClient.exists(key)
    flag
  }

  def del(key:String) ={
    val res = redisClient.del(key)
    //    redisClient.close()
    res
  }

  def expireTime(key:String) ={
    redisClient.expire(key,exTime)
    //    redisClient.close()
  }

  def add(key:String,value:String)={
    val res = redisClient.set(key,value)
    res
  }

  def get(key:String)={
    val res = redisClient.get(key)
    res
  }

  def close()={
    redisClient.close()
  }

  def flush()={

    redisClient.flushDB()
  }

  def keys(pattern :String) ={
    val res = redisClient.hkeys(pattern)
    res
  }

}

object RedisClusterUtil{

  def main(args: Array[String]): Unit = {
    val jedisClusterUtil = new RedisClusterUtil
    val contentId: String = "114246456"
    val contentId2: String = "111878597"
    //    jedisClusterUtil.del(contentId)
    //        jedisClusterUtil.del(contentId2)
    //
    ////    jedisClusterUtil.del(contentId + "update")
    //
    ////    jedisClusterUtil.del(contentId2 + "update")
    //
    //    jedisClusterUtil.sadd(contentId , "{\"id\":\"comment1\",\"topic\":\"P.E is the best\"}")
    //    jedisClusterUtil.sadd(contentId , "{\"id\":\"comment2\",\"topic\":\"Math is the worst\"}")
    //    jedisClusterUtil.add(contentId + "update" , "{\"id\":"+ contentId +",\"url\":\"www.sohu.com/a/"+ contentId+"_userId\"}")
    //
    //    var members = jedisClusterUtil.smembers(contentId)
    //
    //
    //    println("##### contentId  for the first time #####")
    //    import scala.collection.JavaConversions._
    //    for(member <- members){
    //
    //      println(member)
    //    }
    //
    //    jedisClusterUtil.sadd(contentId , "{\"id\":\"comment1\",\"topic\":\"Science is soso I suppose \"}")
    //    jedisClusterUtil.sadd(contentId , "{\"id\":\"comment2\",\"topic\":\"Art is the far from my cell\"}")
    //
    //    jedisClusterUtil.sadd(contentId , "{\"id\":\"comment1\",\"topic\":\"Science is soso I suppose \"}")
    //    jedisClusterUtil.sadd(contentId , "{\"id\":\"comment2\",\"topic\":\"Art is the far from my cell\"}")
    //    jedisClusterUtil.sadd(contentId , "{\"id\":\"comment1\",\"topic\":\"Science is soso I suppose \"}")
    //    jedisClusterUtil.sadd(contentId , "{\"id\":\"comment2\",\"topic\":\"Art is the far from my cell\"}")
    //
    //    jedisClusterUtil.sadd(contentId2 , "{\"id\":\"comment3\",\"topic\":\"Science is soso I suppose \"}")
    //    jedisClusterUtil.sadd(contentId2 , "{\"id\":\"comment4\",\"topic\":\"Art is the far from my cell\"}")
    //    jedisClusterUtil.add(contentId2 + "update" , "{\"id\":"+ contentId2 +",\"url\":\"www.sohu.com/a/"+contentId2+"_userId\"}")
    //
    ////    val update1 = jedisClusterUtil.get(contentId + "update")
    ////    println(s"the update info1=${update1}")
    ////    val update2 = jedisClusterUtil.get(contentId2 + "update")
    ////    println(s"the update info2=${update2}")
    //
    //     members = jedisClusterUtil.smembers(contentId)
    //    val members2 = jedisClusterUtil.smembers(contentId2)
    //
    //    println("##### contentId #####")
    //    import scala.collection.JavaConversions._
    //    for(member <- members){
    //      println(member)
    //    }

    //    println("##### contentId2 #####")
    //    import scala.collection.JavaConversions._
    //    for(member <- members2){
    //      println(member)
    //    }

    //    jedisClusterUtil.flush()

    //    jedisClusterUtil.add(contentId + "update" , "{\"id\":\"asdasdasdasd\",\"url\":\"www.sohu.com/a/"+ contentId+"_userId\"}")
    //    new Thread(
    //      new Runnable {
    //        override def run(): Unit = {
    //          jedisClusterUtil.add(contentId + "update" , "{\"id\":\"heiheihei\",\"url\":\"www.sohu.com/a/"+ contentId+"_userId\"}")
    //        }
    //      }
    //    ).start()
    if(null != jedisClusterUtil.get( s"${contentId}update")){
      println( jedisClusterUtil.get( s"${contentId}update") )
    }
    println(jedisClusterUtil.keys("*update").size())
    jedisClusterUtil.close()
  }

}
