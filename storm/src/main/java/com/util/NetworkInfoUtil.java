package com.util;

import org.apache.commons.codec.binary.Base64;
import sun.misc.BASE64Encoder;

import java.security.MessageDigest;
import java.text.SimpleDateFormat;

/**
 * @Author: lance
 * @Date: 2018/11/30
 */
public class NetworkInfoUtil {

    public static final Base64 base64 = new Base64(76,"".getBytes());

    public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyMMddHHmmss");
    public static final String base_url = "www.xxx.com";
    public static final String msg_version_str = "msg_version";
    public static final String msg_version_1_0 = "v1.0";
    public static final String msg_version_1_1 = "v1.1";
    public static final String msg_type_str = "msg_type";
    public static final String total_pushed_num = "total_pushed_num";
    public static final String record_list = "record_list";
    public static final String article = "article";
    public static final String articleupd = "articleupd";
    public static final String comment = "comment";
    public static final String commentupd = "commentupd";
    public static final String author = "author";
    public static final String authorupd = "authorupd";
    public static final String hashuidupd = "hashuidupd";

    public static final String account_check_base_url ="http://data-apiDemo.com/profile/detail?accountId=";
    public static final String rtpApiBaseUrl = "http://data-apiDemo.com/v2/news/";


    static public String privateKey = "xxx";

    static public String encrypt(String input) {

        try {
            String str = input + privateKey;
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            BASE64Encoder base64en = new BASE64Encoder();
            String newstr=base64en.encode(md5.digest(str.getBytes("utf-8")));
            return newstr;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    static public boolean verify(String input, long timeStamp ,String secret) {
        try {
            return encrypt(input).equals(secret);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    static public boolean verify( String input, String timeStamp, String secret) {
        try {
            return verify(input,Long.valueOf(timeStamp),secret);
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return false;
        }
    }
    public static void main(String[] args){

    }

}
