package com.bolt;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.metrics.MonitorMetrics;
import com.util.HttpUtil;
import com.util.JedisClusterUtil;
import com.util.NetworkInfoUtil;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author: lance
 * @Date: 2018/11/30
 */
public class CommentBolt extends BaseBasicBolt {

    JedisClusterUtil jedisClusterUtil = JedisClusterUtil.getInstance();

    public String regx = "\\S/(\\d+)(_(\\d+))?";

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            JSONObject tmp = JSON.parseObject(input.getString(0));
            Pattern pattern = Pattern.compile(regx);
            String ORIGINURL = tmp.getString("topicUrl");//文章URL
            Matcher matcher = pattern.matcher(ORIGINURL);
            if (matcher.find()){
                //rtp api invoked first
                String ARTICLEID = matcher.group(1).split("_")[0]; //get the article Id

                String api_res = HttpUtil.doGet(NetworkInfoUtil.rtpApiBaseUrl + ARTICLEID);
                if (api_res != null) {
                    JSONObject api_res_json = JSON.parseObject(api_res); //api返回的结果
                    String userId = api_res_json.getString("userId");
                    //account check secondly
                    String account_check_url = NetworkInfoUtil.account_check_base_url + userId;
                    String api_check_res = HttpUtil.doGet(account_check_url);
                    if (null != api_check_res) {
                        JSONObject account_check_res = JSON.parseObject(api_check_res);
                        int agreement = 0;
                        if (null != account_check_res.getInteger("agreement")) {
                            agreement = account_check_res.getInteger("agreement");
                        }
                        if (agreement == 1 || agreement == 2 || agreement == 3) { //  the meet of authorized account
                            String POSTID = tmp.getString("id");//评论ID

                            String DOMAIN = "www.xxx.com";//
                            String POSTCHANNEL_ = tmp.getString("from");
                            String POSTCHANNEL = "1";
                            if ("pc".equals(POSTCHANNEL_)) {
                                POSTCHANNEL = "1";
                            } else if ("mobile".equals(POSTCHANNEL_)) {
                                POSTCHANNEL = "2";
                            } else if ("client".equals(POSTCHANNEL_)) {
                                POSTCHANNEL = "3";
                            }
                            String ARTICLETYPE = "0";//
                            String POSTTIME = tmp.getString("addTime");//
                            String POSTCONT = tmp.getString("content");//
                            String ARTICLEID_ = ARTICLEID;
                            POSTID = NetworkInfoUtil.base64.encodeToString(POSTID.getBytes());//base64
                            ORIGINURL = NetworkInfoUtil.base64.encodeToString(ORIGINURL.getBytes());//base64
                            POSTCONT = NetworkInfoUtil.base64.encodeToString(POSTCONT.getBytes());//base64

                            JSONObject res = new JSONObject();
                            res.put("POSTID", POSTID);
                            res.put("DOMAIN", DOMAIN);
                            res.put("POSTCHANNEL", POSTCHANNEL);
                            res.put("ARTICLETYPE", ARTICLETYPE);
                            res.put("ORIGINURL", ORIGINURL);
                            res.put("POSTTIME", POSTTIME);

                            JSONObject to_update_comment_info = res;
                            to_update_comment_info.put(NetworkInfoUtil.msg_version_str, NetworkInfoUtil.msg_version_1_1);
                            to_update_comment_info.put(NetworkInfoUtil.msg_type_str, NetworkInfoUtil.commentupd);
                            jedisClusterUtil.sadd("wxb:article:add:" + ARTICLEID_, to_update_comment_info.toJSONString());//add comment-info to redis
                            res.put("POSTCONT", POSTCONT);
                            res.put(NetworkInfoUtil.msg_version_str, NetworkInfoUtil.msg_version_1_1);
                            res.put(NetworkInfoUtil.msg_type_str, NetworkInfoUtil.comment);

//                        System.out.println(res.toJSONString());
                            collector.emit(new Values(null, res.toJSONString()));
                        }
                    }

                }

            }
        } catch (Exception e) {
            new MonitorMetrics(input.toString());
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key", "message"));
    }
}
