package com.bolt;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
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

/**
 * @Author: lance
 * @Date: 2018/11/29
 */
public class ArticleBolt extends BaseBasicBolt {

    JedisClusterUtil jedisClusterUtil = JedisClusterUtil.getInstance();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            JSONObject tmp = JSON.parseObject(input.getString(0));
            JSONObject jsonObject = (JSONObject) tmp.get("mpNews");
            int auditStatus = jsonObject.getInteger("auditStatus");
            String userId = jsonObject.getString("userId");

            //account check first
            String account_check_url = NetworkInfoUtil.account_check_base_url + userId;
            String api_check_res = HttpUtil.doGet(account_check_url);

            if (null != api_check_res) {
                JSONObject account_check_res = JSON.parseObject(api_check_res);
                int agreement = 0;
                if (null != account_check_res.getInteger("agreement")) {
                    agreement = account_check_res.getInteger("agreement");
                }
                if (agreement == 1 || agreement == 2 || agreement == 3) { //  the meet of authorized account
                    String id_ = jsonObject.getString("id");
                    StringBuilder sb = new StringBuilder();
                    String ORIGINURL = sb.append(NetworkInfoUtil.base_url).append("/a/").append(id_).append("_").append(userId).toString();
                    String DOMAIN = NetworkInfoUtil.base_url;
                    String CHANNEL_ = jsonObject.getString("channelId");
                    switch (CHANNEL_) {
                        case "10":
                            CHANNEL_ = "军事";
                            break;
                        case "11":
                            CHANNEL_ = "体育";
                            break;
                        case "12":
                            CHANNEL_ = "文化";
                            break;
                        case "13":
                            CHANNEL_ = "历史";
                            break;
                        case "14":
                            CHANNEL_ = "艺术";
                            break;
                        case "15":
                            CHANNEL_ = "财经";
                            break;
                    }
                    String TITLE = jsonObject.getString("title");
                    String PUBLISHTIME = NetworkInfoUtil.sdf.format(jsonObject.getLong("postTime"));
                    String TEXT = jsonObject.getString("content");
                    String INSTRUCTION = "0"; //todo
                    JSONArray LABEL = new JSONArray();
                    JSONObject jsonObject1 = new JSONObject();
                    jsonObject1.put("name", "CHANNEL");
                    jsonObject1.put("value", NetworkInfoUtil.base64.encodeToString(CHANNEL_.getBytes()));//base64
                    LABEL.add(jsonObject1);
                    jsonObject1 = new JSONObject();
                    jsonObject1.put("name", "INSTRUCTION");
                    jsonObject1.put("value", NetworkInfoUtil.base64.encodeToString(INSTRUCTION.getBytes()));//base64
                    LABEL.add(jsonObject1);
                    String DOCTYPE = null;
                    int DOCTYPE_ = jsonObject.getInteger("type");
                    if (DOCTYPE_ == 1) {
                        DOCTYPE = "文本类型";
                    } else if (DOCTYPE_ == 2) {
                        DOCTYPE = "图文类型";
                    } else if (DOCTYPE_ == 3) {
                        DOCTYPE = "图集类型";
                        ORIGINURL = sb.append(NetworkInfoUtil.base_url).append("/pictue/").append(id_).toString();
                    } else if (DOCTYPE_ == 4) {
                        DOCTYPE = "视频类型";
                    } else if (DOCTYPE_ == 5) {
                        DOCTYPE = "视频文章";
                    }
                    String CHANNEL = "1";
                    String SNAPSHOTTIME = null;
                    if (auditStatus == 4 && null != jsonObject.getLong("auditTime")) {
                        SNAPSHOTTIME = NetworkInfoUtil.sdf.format(jsonObject.getLong("auditTime"));
                    } else if (auditStatus == 4 && null == jsonObject.getLong("auditTime")) {
                        SNAPSHOTTIME = NetworkInfoUtil.sdf.format(jsonObject.getLong("createdTime"));
                    }
                    if (auditStatus != 4) {
                        ORIGINURL = "-1";
                        CHANNEL = "-1";
                        //若文章未通过审核，PUBLISHTIME为空,则用createdTime
                        PUBLISHTIME = NetworkInfoUtil.sdf.format(jsonObject.getLong("createdTime"));
                        if (null == SNAPSHOTTIME)
                            SNAPSHOTTIME = NetworkInfoUtil.sdf.format(jsonObject.getLong("createdTime"));
                    }

                    String TextWithoutHTML="";
                    if (null != TITLE)
                        TITLE = NetworkInfoUtil.base64.encodeToString(TITLE.getBytes());//base64
                    if (null != TEXT) {
                        TextWithoutHTML = TEXT.replaceAll("<[.[^>]]*>","");
                        TextWithoutHTML = NetworkInfoUtil.base64.encodeToString(TextWithoutHTML.getBytes());//base64
                    }
                    if (null != ORIGINURL)
                        ORIGINURL = NetworkInfoUtil.base64.encodeToString(ORIGINURL.getBytes());//base64
                    if (null != DOCTYPE)
                        DOCTYPE = NetworkInfoUtil.base64.encodeToString(DOCTYPE.getBytes()); //base64
                    String AUTHORID = null;
                    if(null != userId)
                        AUTHORID = NetworkInfoUtil.base64.encodeToString(userId.getBytes());//base64
                    String ARTICLEID = null;
                    if(null!=id_)
                        ARTICLEID = NetworkInfoUtil.base64.encodeToString(id_.getBytes());//base64
                    JSONObject res = new JSONObject();
                    res.put("ARTICLEID", ARTICLEID);

                    res.put("ORIGINURL", ORIGINURL);
                    res.put("DOMAIN", DOMAIN);
                    res.put("CHANNEL", CHANNEL);
                    res.put("SNAPSHOTTIME", SNAPSHOTTIME);//

                    JSONObject to_update_article_info = res;
                    to_update_article_info.put(NetworkInfoUtil.msg_version_str, NetworkInfoUtil.msg_version_1_1);
                    to_update_article_info.put(NetworkInfoUtil.msg_type_str, NetworkInfoUtil.articleupd);
                    //add article_id to redis
                    jedisClusterUtil.sadd("wxb:article:add:" + id_, "");
                    jedisClusterUtil.expireTime("wxb:article:add:" + id_, JedisClusterUtil.expireTime);

                    //add article updateInfo to redis
                    jedisClusterUtil.add("wxb:article:update:" + id_, to_update_article_info.toJSONString());
                    jedisClusterUtil.expireTime("wxb:article:update:" + id_, JedisClusterUtil.expireTime);

                    res.put("TITLE", TITLE);
                    res.put("PUBLISHTIME", PUBLISHTIME);
                    res.put("TEXT", TextWithoutHTML);
                    res.put("LABEL", LABEL);
                    res.put("AUTHORID", AUTHORID); //new added,not needed
                    res.put(NetworkInfoUtil.msg_version_str, NetworkInfoUtil.msg_version_1_1);
                    res.put(NetworkInfoUtil.msg_type_str, NetworkInfoUtil.article);
//                    System.out.println(res.toJSONString());
                    collector.emit(new Values(null, res.toJSONString()));
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
