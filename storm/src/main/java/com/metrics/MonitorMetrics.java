package com.metrics;
import com.util.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Properties;

/**
 * @Author: lance
 * @Date: 2018/12/5
 */

public class MonitorMetrics {

    private final SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    private final SimpleDateFormat yyyyMMddHHmm = new SimpleDateFormat("yyyyMMddHHmm");
    private final static String ISO_DATETIME_FORMAT_PATTERN = "yyyy-MM-dd'T'HH:mm";

    public MonitorMetrics(final String log) {
        Properties topicProps = new Properties();
        topicProps.put("acks", "0");
        topicProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        topicProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        topicProps.put("bootstrap.servers", "pdn096224.heracles.sohuno.com:9092,kfk013217.heracles.sohuno.com:9092,pdn096223.heracles.sohuno.com:9092,"+
                "pdn096229.heracles.sohuno.com:9092, pdn096228.heracles.sohuno.com:9092,pdn096226.heracles.sohuno.com:9092,"+
                "pdn096230.heracles.sohuno.com:9092,pdn096225.heracles.sohuno.com:9092,kfk013219.heracles.sohuno.com:9092,"+
                "pdn096222.heracles.sohuno.com:9092,pdn096221.heracles.sohuno.com:9092,kfk013220.heracles.sohuno.com:9092");
        final String topic = Constants.TEST_TOPIC;
        KafkaProducer<String, String> monitorProducer = new KafkaProducer<>(topicProps);
        ProducerRecord<String, String> monitorRecord = new ProducerRecord<>(topic, log);
        monitorProducer.send(monitorRecord);
    }


    public static void main(String[] args) {

    }
}
