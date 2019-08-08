package cn.sunss.learn;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * @ClassName: MyKafkaProducer
 * @Description: kafka生产者
 * @Author sunsongsong
 * @Date 2019/8/6 21:39
 * @Version 1.0
 */
public class MyKafkaProducer {



    public static void main(String[] args) {
        Random random = new Random();
        String[] arrays = new String[]{"hello world","hadoop hive","flume hadoop"};

        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String,String>(props);

        for (int i = 0; i < 100; i++){
            String line = arrays[random.nextInt(arrays.length)];
            producer.send(new ProducerRecord<String, String>("stormTopic2", line));
        }
        producer.close();

    }


}
