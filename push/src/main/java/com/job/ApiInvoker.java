package com.job;


import com.alibaba.fastjson.JSONObject;
import com.util.Constants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.*;

/**
 * @Author: lance
 * @Date: 2018/12/7
 */
public class ApiInvoker {

    public static void main(String[] args) {
        consumer();
    }

    public static void send(String msg) {
        Properties config = new Properties();
        config.put("bootstrap.servers", "xxx");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//		config.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024*1024*5);
        //往kafka服务器提交消息间隔时间，0则立即提交不等待
        config.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        KafkaProducer<String, String> producer = new KafkaProducer<>(config);
        producer.send(new ProducerRecord<>(Constants.TEST_TOPIC, msg));
    }

    public static void consumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "xxx");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "wxb_" + new Date());
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "latest");
        props.put("session.timeout.ms", "30000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(Constants.WXB_TOPIC));
        System.out.println("about to consume what wxb needs ...");

        JSONObject article;
        JSONObject articleupd;
        JSONObject comment;
        JSONObject commentupd;
        JSONObject author;
        JSONObject authorupd;
        JSONObject hashuidupd;

        List<JSONObject> article_list;
        List<JSONObject> articleupd_list;
        List<JSONObject> comment_list;
        List<JSONObject> commentupd_list;
        List<JSONObject> author_list;
        List<JSONObject> authorupd_list;
        List<JSONObject> hashuidupd_list;

        while (true) {
            article = new JSONObject();
            articleupd = new JSONObject();
            comment = new JSONObject();
            commentupd = new JSONObject();
            author = new JSONObject();
            authorupd = new JSONObject();
            hashuidupd = new JSONObject();

            article_list = new ArrayList();
            articleupd_list = new ArrayList<>();
            comment_list = new ArrayList<>();
            commentupd_list = new ArrayList<>();
            author_list = new ArrayList<>();
            authorupd_list = new ArrayList<>();
            hashuidupd_list = new ArrayList<>();
            try {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    JSONObject msg = JSONObject.parseObject(record.value());
                    if (null != msg) {
                        String msg_type = msg.getString(Constants.msg_type_str);
                        switch (msg_type) {
                            case Constants.selfmedia_article:
                                msg.remove(Constants.msg_type_str);
                                msg.remove(Constants.msg_version_str);
                                article_list.add(msg);
                                break;
                            case Constants.selfmedia_articleupd:
                                msg.remove(Constants.msg_type_str);
                                msg.remove(Constants.msg_version_str);
                                articleupd_list.add(msg);
                                break;
                            case Constants.selfmedia_comment:
                                msg.remove(Constants.msg_type_str);
                                msg.remove(Constants.msg_version_str);
                                comment_list.add(msg);
                                break;
                            case Constants.selfmedia_commentupd:
                                msg.remove(Constants.msg_type_str);
                                msg.remove(Constants.msg_version_str);
                                commentupd_list.add(msg);
                                break;
                            case Constants.selfmedia_author:
                                msg.remove(Constants.msg_type_str);
                                msg.remove(Constants.msg_version_str);
                                author_list.add(msg);
                                break;
                            case Constants.selfmedia_authorupd:
                                msg.remove(Constants.msg_type_str);
                                msg.remove(Constants.msg_version_str);
                                authorupd_list.add(msg);
                                break;
                            case Constants.selfmedia_hashuidupd:
                                msg.remove(Constants.msg_type_str);
                                msg.remove(Constants.msg_version_str);
                                hashuidupd_list.add(msg);
                                break;
                        }
                    }

                }

                article.put(Constants.msg_version_str, Constants.msg_version_1_1);
                article.put(Constants.msg_type_str, Constants.selfmedia_article);
                article.put(Constants.total_pushed_num, article_list.size());
                article.put(Constants.record_list, article_list);

                articleupd.put(Constants.msg_version_str, Constants.msg_version_1_1);
                articleupd.put(Constants.msg_type_str, Constants.selfmedia_articleupd);
                articleupd.put(Constants.total_pushed_num, articleupd_list.size());
                articleupd.put(Constants.record_list, articleupd_list);

                comment.put(Constants.msg_version_str, Constants.msg_version_1_1);
                comment.put(Constants.msg_type_str, Constants.selfmedia_comment);
                comment.put(Constants.total_pushed_num, comment_list.size());
                comment.put(Constants.record_list, comment_list);

                commentupd.put(Constants.msg_version_str, Constants.msg_version_1_1);
                commentupd.put(Constants.msg_type_str, Constants.selfmedia_commentupd);
                commentupd.put(Constants.total_pushed_num, commentupd_list.size());
                commentupd.put(Constants.record_list, commentupd_list);

                author.put(Constants.msg_version_str, Constants.msg_version_1_1);
                author.put(Constants.msg_type_str, Constants.selfmedia_author);
                author.put(Constants.total_pushed_num, author_list.size());
                author.put(Constants.record_list, author_list);

                authorupd.put(Constants.msg_version_str, Constants.msg_version_1_1);
                authorupd.put(Constants.msg_type_str, Constants.selfmedia_authorupd);
                authorupd.put(Constants.total_pushed_num, authorupd_list.size());
                authorupd.put(Constants.record_list, authorupd_list);

                hashuidupd.put(Constants.msg_version_str, Constants.msg_version_1_0);
                hashuidupd.put(Constants.msg_type_str, Constants.selfmedia_hashuidupd);
                hashuidupd.put(Constants.total_pushed_num, hashuidupd_list.size());
                hashuidupd.put(Constants.record_list, hashuidupd_list);
//
                try {

                    if (article_list.size() != 0) {
                        send(article.toJSONString());
                    }
                    if (articleupd_list.size() != 0) {
                        send(articleupd.toJSONString());
                    }
                    if (comment_list.size() != 0) {
                        send(comment.toJSONString());
                    }
                    if (commentupd_list.size() != 0) {
                        send(commentupd.toJSONString());
                    }
                    if (author_list.size() != 0) {
                        send(author.toJSONString());
                    }
                    if (authorupd_list.size() != 0) {
                        send(authorupd.toJSONString());
                    }
                    if (hashuidupd_list.size() != 0) {
                        send(hashuidupd.toJSONString());
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

                Thread.sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
