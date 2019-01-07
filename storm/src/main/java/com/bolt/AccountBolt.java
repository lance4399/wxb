package com.bolt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.util.HttpUtil;
import com.util.MD5Utils;
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
public class AccountBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            JSONObject tmp = JSON.parseObject(input.getString(0));
//            System.out.println(tmp.toJSONString());
            String AUTHORID = tmp.getString("id");
            //account check first

            String account_check_url = NetworkInfoUtil.account_check_base_url + AUTHORID;
            String api_check_res = HttpUtil.doGet(account_check_url);
            if (null != api_check_res) {
                JSONObject account_check_res = JSON.parseObject(api_check_res);
                int agreement = 0;
                if (null != account_check_res.getInteger("agreement")) {
                    agreement = account_check_res.getInteger("agreement");
                }

                if (agreement == 1 || agreement == 2 || agreement == 3) { //  the meet of authorized account
                    AUTHORID = NetworkInfoUtil.base64.encodeToString(AUTHORID.getBytes());//base64
                    String DOMAIN = "www.sohu.com";
                    String NAME = tmp.getString("nickName");
                    NAME = NetworkInfoUtil.base64.encodeToString(NAME.getBytes());//base64

                    int USERTYPE = 1;
                    //add
                    JSONObject res = new JSONObject();
                    res.put("AUTHORID", AUTHORID);
                    res.put("DOMAIN", DOMAIN);
                    res.put("NAME", NAME);
                    res.put(NetworkInfoUtil.msg_version_str, NetworkInfoUtil.msg_version_1_1);
                    res.put(NetworkInfoUtil.msg_type_str, NetworkInfoUtil.author);
                    collector.emit(new Values(null, res.toJSONString()));

                    //update
                    JSONObject updatedAccountInfo = new JSONObject();
                    updatedAccountInfo.put("AUTHORID", AUTHORID);
                    updatedAccountInfo.put("DOMAIN", DOMAIN);
                    updatedAccountInfo.put(NetworkInfoUtil.msg_version_str, NetworkInfoUtil.msg_version_1_1);
                    updatedAccountInfo.put(NetworkInfoUtil.msg_type_str, NetworkInfoUtil.authorupd);
                    collector.emit(new Values(null, updatedAccountInfo.toJSONString()));

                    // md5 hash
                    JSONObject hashuidInfo = new JSONObject();
                    //the update of account encryption (md5)
                    String HASHUID = MD5Utils.MD5Encode(AUTHORID);

                    hashuidInfo.put("HASHUID", HASHUID);
                    hashuidInfo.put("USERTYPE", USERTYPE);
                    hashuidInfo.put(NetworkInfoUtil.msg_version_str, NetworkInfoUtil.msg_version_1_0);
                    hashuidInfo.put(NetworkInfoUtil.msg_type_str, NetworkInfoUtil.hashuidupd);

//                    System.out.println(res.toJSONString());
                    collector.emit(new Values(null, hashuidInfo.toJSONString()));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key", "message"));
    }
}
