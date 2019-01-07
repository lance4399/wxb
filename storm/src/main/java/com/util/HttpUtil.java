package com.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author: lance
 * @Date: 2018/12/3
 */

public class HttpUtil {

    public static String doGet(String httpurl) {
        HttpURLConnection connection = null;
        InputStream is = null;
        BufferedReader br = null;
        String result = null;// 返回结果字符串
        try {
            // 创建远程url连接对象
            URL url = new URL(httpurl);
            // 通过远程url连接对象打开一个连接，强转成httpURLConnection类
            connection = (HttpURLConnection) url.openConnection();
            // 设置连接方式：get
            connection.setRequestMethod("GET");
            // 设置连接主机服务器的超时时间：15000毫秒
            connection.setConnectTimeout(15000);
            // 设置读取远程返回的数据时间：60000毫秒
            connection.setReadTimeout(60000);
            // 发送请求
            connection.connect();
            // 通过connection连接，获取输入流
            if (connection.getResponseCode() == 200) {
                is = connection.getInputStream();
                // 封装输入流is，并指定字符集
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                // 存放数据
                StringBuffer sbf = new StringBuffer();
                String temp = null;
                while ((temp = br.readLine()) != null) {
                    sbf.append(temp);
                    sbf.append("\r\n");
                }
                result = sbf.toString();
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            connection.disconnect();
        }

        return result;
    }

    public static String doPost(String httpUrl, String param) {

        HttpURLConnection connection = null;
        InputStream is = null;
        OutputStream os = null;
        BufferedReader br = null;
        String result = null;
        try {
            URL url = new URL(httpUrl);
            // 通过远程url连接对象打开连接
            connection = (HttpURLConnection) url.openConnection();
            // 设置连接请求方式
            connection.setRequestMethod("POST");
            // 设置连接主机服务器超时时间：15000毫秒
            connection.setConnectTimeout(15000);
            // 设置读取主机服务器返回数据超时时间：60000毫秒
            connection.setReadTimeout(60000);

            // 默认值为：false，当向远程服务器传送数据/写数据时，需要设置为true
            connection.setDoOutput(true);
            // 默认值为：true，当前向远程服务读取数据时，设置为true，该参数可有可无
            connection.setDoInput(true);
            // 设置传入参数的格式:请求参数应该是 name1=value1&name2=value2 的形式。
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            // 设置鉴权信息：Authorization: Bearer da3efcbf-0845-4fe3-8aba-ee040be542c0
            connection.setRequestProperty("Authorization", "Bearer da3efcbf-0845-4fe3-8aba-ee040be542c0");
            // 通过连接对象获取一个输出流
            os = connection.getOutputStream();
            // 通过输出流对象将参数写出去/传输出去,它是通过字节数组写出的
            os.write(param.getBytes());
            // 通过连接对象获取一个输入流，向远程读取
            if (connection.getResponseCode() == 200) {

                is = connection.getInputStream();
                // 对输入流对象进行包装:charset根据工作项目组的要求来设置
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));

                StringBuffer sbf = new StringBuffer();
                String temp = null;
                // 循环遍历一行一行读取数据
                while ((temp = br.readLine()) != null) {
                    sbf.append(temp);
                    sbf.append("\r\n");
                }
                result = sbf.toString();
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != os) {
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // 断开与远程地址url的连接
            connection.disconnect();
        }
        return result;
    }


    public static void main(String[] args){


        String regx = "^/(n|a|o|(zo)|(news/a))/(\\d+)(_(\\d+))?/?$"; // 正文页
        String input = "http://www.sohu.com/a/893127_748283";
        Pattern pattern = Pattern.compile(regx);
        Matcher matcher = pattern.matcher(input);
        if(matcher.find()){
            System.out.println("yes");
        }else{
            System.out.println("no");
        }


    }


    public static void test(){
        String rtpApiBaseUrl = "http://data-api.mp.sohuno.com/v2/news/";
        String url = rtpApiBaseUrl + "190493652";
        String tmp = HttpUtil.doGet(url);
        JSONObject api_res_json = JSON.parseObject(tmp);

        String id_ = api_res_json.getString("id");
        String userId = api_res_json.getString("userId");
//        String ARTICLEID = NetworkInfoUtil.encoder.encodeToString(id_.getBytes());//base64
        String ARTICLEID = id_;//base64
        StringBuilder sb = new StringBuilder();

        String ORIGINURL = sb.append(NetworkInfoUtil.base_url).append("/a/").append(id_).append("_").append(userId).toString();
        String DOMAIN = NetworkInfoUtil.base_url;
        String CHANNEL = "1";

        String TITLE = api_res_json.getString("title");
//        if (null != TITLE) TITLE = NetworkInfoUtil.encoder.encodeToString(TITLE.getBytes());//base64

        String PUBLISHTIME = NetworkInfoUtil.sdf.format(api_res_json.getLong("postTime"));
        String TEXT = api_res_json.getString("content");
//        if (null != TEXT) TEXT = NetworkInfoUtil.encoder.encodeToString(TEXT.getBytes());//base64

        String INSTRUCTION = "0"; //todo
        JSONArray LEBAEL = new JSONArray();
        JSONObject jsonObject1 = new JSONObject();
        jsonObject1.put("name", "CHANNEL");
//        jsonObject1.put("value", NetworkInfoUtil.encoder.encodeToString( CHANNEL.getBytes() ) );//base64
        jsonObject1.put("value",  CHANNEL  );//base64
        LEBAEL.add(jsonObject1);
        jsonObject1 = new JSONObject();
        jsonObject1.put("name", "INSTRUCTION");
//        jsonObject1.put("value", NetworkInfoUtil.encoder.encodeToString( INSTRUCTION.getBytes() ) );//base64
        jsonObject1.put("value", INSTRUCTION );//base64
        LEBAEL.add(jsonObject1);
        String STATE = "1"; //稿件状态(1->通过，4->删除),todo

        String UPLOADTIME = null; //上稿时间(指令文章填上os的时间，非指令文章不填写),todo
        long time = 1543391769115l;
        UPLOADTIME = NetworkInfoUtil.sdf.format(time);
        String DOCTYPE = null;
        int DOCTYPE_ = api_res_json.getInteger("type");
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
//        ORIGINURL = NetworkInfoUtil.encoder.encodeToString(ORIGINURL.getBytes());//base64
//        DOCTYPE = NetworkInfoUtil.encoder.encodeToString(DOCTYPE.getBytes()); //base64
        String SNAPSHOTTIME = null;
        if ( null != api_res_json.getLong("auditTime")) {
            SNAPSHOTTIME = NetworkInfoUtil.sdf.format(api_res_json.getLong("auditTime"));
        }
//        if (auditStatus != 4) {
//            ORIGINURL = "-1";
//            CHANNEL = "-1";
//            PUBLISHTIME = "";
//            STATE = "3";
//            UPLOADTIME = "19700101000000";
//        }

        JSONObject res = new JSONObject();
        res.put("ARTICLEID", ARTICLEID);
        res.put("ORIGINURL", ORIGINURL);
        res.put("DOMAIN", DOMAIN);
        res.put("CHANNEL", CHANNEL);
        res.put("TITLE", TITLE);
        res.put("PUBLISHTIME", PUBLISHTIME);
        res.put("TEXT", TEXT);
        res.put("LEBAEL", LEBAEL);
        res.put("STATE", STATE);
        res.put("UPLOADTIME", UPLOADTIME);
        res.put("DOCTYPE", DOCTYPE);
        res.put("SNAPSHOTTIME", SNAPSHOTTIME);
        for(String key : res.keySet()){
            System.out.println(key +"->" + res.get(key));
        }
    }
}
