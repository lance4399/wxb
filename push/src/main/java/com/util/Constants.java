package com.util;

import java.text.SimpleDateFormat;

/**
 * @Author: lance
 * @Date: 2018/12/5
 */
public class Constants {

    public static final long INTERVAL = 5*60*1000;

    public static final String TEST_TOPIC = "mediaai_logcollect_test";
    public static final String WXB_TOPIC = "mediaai_gov_wxbrep";

    public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyMMddHHmmss");
    public static final String base_url = "www.sohu.com";

    public static final String msg_version_str = "msg_version";
    public static final String msg_version_1_0 = "v1.0";
    public static final String msg_version_1_1 = "v1.1";
    public static final String msg_type_str = "msg_type";
    public static final String total_pushed_num = "total_pushed_num";
    public static final String record_list = "record_list";
    public static final String selfmedia_article = "selfmedia.article";
    public static final String selfmedia_articleupd = "selfmedia.articleupd";
    public static final String selfmedia_comment = "selfmedia.comment";
    public static final String selfmedia_commentupd = "selfmedia.commentupd";
    public static final String selfmedia_author = "selfmedia.author";
    public static final String selfmedia_authorupd = "selfmedia.authorupd";
    public static final String selfmedia_hashuidupd = "selfmedia.hashuidupd";

    public static final String account_check_base_url ="http://data-api.mp.sohuno.com/profile/detail?accountId=";

}
