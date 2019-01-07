package com.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * @Author: lance
 * @Date: 2018/12/1
 */
public class JedisClusterUtil implements Serializable{

    private JedisClusterUtil(){}

    private static JedisCluster jedisCluster;

    public static final int expireTime = 48*60*60;

    private static final JedisClusterUtil jedisClusterUtil = new JedisClusterUtil();
    static {
        Set<HostAndPort> nodes = new HashSet<>();
        nodes.add(new HostAndPort("node1",20001));
        nodes.add(new HostAndPort("node2",20002));
        nodes.add(new HostAndPort("node3",20003));


        GenericObjectPoolConfig jedisPoolConfig = new GenericObjectPoolConfig();
        // 最大空闲连接数, 默认8个// 最大空闲连接数, 默认8个
        jedisPoolConfig.setMaxIdle(10);
        // 最大连接数, 默认8个
        jedisPoolConfig.setMaxTotal(50);
        //最小空闲连接数, 默认0
        jedisPoolConfig.setMinIdle(1);
        // 获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
        jedisPoolConfig.setMaxWaitMillis(2000); // 设置2秒
        //对拿到的connection进行validateObject校验
        jedisPoolConfig.setTestOnBorrow(true);

        jedisCluster = new JedisCluster(nodes, 2000 , 1000 , 5 , "password", jedisPoolConfig);

    }

    public static JedisClusterUtil getInstance(){
        return jedisClusterUtil;
    }


    public void sadd(String key ,String ...value){
        jedisCluster.sadd(key,value);

    }


    public Set<String> smembers(String key ){
        Set<String> res = jedisCluster.smembers(key);
        return res;
    }

    public void expireTime(String key,int seconds ){
        jedisCluster.expire(key,seconds);

    }

    public void add(String key,String value){

        jedisCluster.set(key,value);
    }



    public void close(){
        try {
            jedisCluster.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String get(String key){
        String res = jedisCluster.get(key);
//        try {
//            jedisCluster.close();
//            return res;
//        } catch (IOException e) {
//            e.printStackTrace();
//            return null;
//        }
        return res;
    }

    public TreeSet<String> keys(String pattern){
        TreeSet<String> res = new TreeSet<>();

        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
        for(String k : clusterNodes.keySet()){
            JedisPool pool = clusterNodes.get(k);
            Jedis jedis = pool.getResource();
            try{
                res.addAll( jedis.keys(pattern) );
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return res;
    }


    public static void main(String[] args){
        String key = "20181204test";
        JedisClusterUtil jedisClusterUtil = JedisClusterUtil.getInstance();
//        System.out.println( jedisClusterUtil.smembers(key) );
        String contentId="114246456";
//        jedisClusterUtil.add(contentId + "update","{\"id\":123456,\"topic\":\"value2\"}");

        System.out.println(jedisClusterUtil.keys("114246456"));

        jedisClusterUtil.close();

//        System.out.println( JedisClusterUtil.getInstance().keys( key ) );
    }



}
