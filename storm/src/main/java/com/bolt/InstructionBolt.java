package com.bolt;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.metrics.MonitorMetrics;
import com.util.HttpUtil;
import com.util.NetworkInfoUtil;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * @Author: lance
 * @Date: 2018/11/30
 */
public class InstructionBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            JSONObject tmp = JSON.parseObject(input.getString(0));
            JSONObject res = new JSONObject();
            if (null != tmp.getString("mpId")) {
                String mpId = tmp.getString("mpId");//
                String api_res = HttpUtil.doGet(NetworkInfoUtil.rtpApiBaseUrl + mpId);
                //data coming from rtp-api
                if (null != api_res) {
                    JSONObject api_res_json = JSON.parseObject(api_res); //api返回的结果
                    String id_ = api_res_json.getString("id");
                    String ARTICLEID = NetworkInfoUtil.base64.encodeToString(id_.getBytes());//base64
                    res.put("ARTICLEID", ARTICLEID);
                    String DOMAIN = NetworkInfoUtil.base_url;
                    String CHANNEL_ = tmp.getString("channel");
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

                    }
                    String TITLE = api_res_json.getString("title");
                    String PUBLISHTIME = NetworkInfoUtil.sdf.format(api_res_json.getLong("postTime"));
                    String TEXT = api_res_json.getString("content");
                    String INSTRUCTION = tmp.getString("messageType");//
                    JSONArray LABEL = new JSONArray();
                    JSONObject jsonObject1 = new JSONObject();
                    jsonObject1.put("name", "CHANNEL");
                    jsonObject1.put("value", NetworkInfoUtil.base64.encodeToString(CHANNEL_.getBytes()));//base64
                    LABEL.add(jsonObject1);
                    jsonObject1 = new JSONObject();
                    jsonObject1.put("name", "INSTRUCTION");
                    jsonObject1.put("value", NetworkInfoUtil.base64.encodeToString(INSTRUCTION.getBytes())  );//base64
                    LABEL.add(jsonObject1);

                    String DOCTYPE = null;
                    int DOCTYPE_ = api_res_json.getInteger("type");
                    if (DOCTYPE_ == 1) {
                        DOCTYPE = "文本类型";
                    } else if (DOCTYPE_ == 2) {
                        DOCTYPE = "图文类型";
                    } else if (DOCTYPE_ == 3) {
                        DOCTYPE = "图集类型";
                    } else if (DOCTYPE_ == 4) {
                        DOCTYPE = "视频类型";
                    } else if (DOCTYPE_ == 5) {
                        DOCTYPE = "视频文章";
                    }

                    res.put("DOMAIN", DOMAIN);
                    res.put("CHANNEL", "1");
                    res.put("TITLE", TITLE);
                    res.put("PUBLISHTIME", PUBLISHTIME);
                    res.put("TEXT", TEXT);
                    res.put("LABEL", LABEL);
                    res.put("DOCTYPE", DOCTYPE);
                    res.put(NetworkInfoUtil.msg_version_str, NetworkInfoUtil.msg_version_1_1);
                    res.put(NetworkInfoUtil.msg_type_str, NetworkInfoUtil.article);

                    if (null != tmp.getString("link")) {
                        String ORIGINURL = tmp.getString("link");//
                        res.put("ORIGINURL", ORIGINURL);
                    }
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
