package com.imooc.flink.java.project;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * step 1
 * 日志生产者,发送到 kafka
 */
public class PKKafkaProducer {
    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop000:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        String topic = "pktest";
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        //通过死循环一直不停往kafka的broker里面生产数据
        while (true){
            StringBuilder builder = new StringBuilder();
            builder.append("imooc").append("\t");
            builder.append("CN").append("\t");
            builder.append(getLevels()).append("\t");
            builder.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("\t");
            builder.append(getIps()).append("\t");
            builder.append(getDomain()).append("\t");
            builder.append(getTraffic()).append("\t");
            System.out.println(builder.toString());
            //发送到kafka
            producer.send(new ProducerRecord<String,String>(topic,builder.toString()));
            Thread.sleep(2000);
        }
    }

    //流量
    public static int getTraffic(){
        return new Random().nextInt(10000);
    }
    //域名
    public static String getDomain(){
            String[]  domain = new String[]{
                    "v1.go2yd.com",
                    "v2.go2yd.com",
                    "v3.go2yd.com",
                    "v4.go2yd.com",
                    "vmi.go2yd.com",
            };
            return domain[new Random().nextInt(domain.length)];

    }
    //生产ip数据
    public static String getIps(){
        String[]  ips = new String[]{
                "223.104.18.110",
                "113.101.75.194",
                "27.17.127.135",
                "183.225.139.16",
                "112.1.66.34",
                "175.148.211.190",
                "183.227.58.21",
                "59.83.198.84",
                "117.28.38.28",
                "117.59.39.169"
        };
        return ips[new Random().nextInt(ips.length)];
    }
    //生产level数据
    public static String getLevels(){
        String[]  levels = new String[]{"M","E"};
        return levels[new Random().nextInt(levels.length)];
    }
}
