package com.imooc.flink.java.course02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用Java API来开发Flink的实时处理程序
 * WC 统计的数据，来源于socket
 * 运行前，先运行 nc -l -p 9999
 */
public class StreamingWCJavaApp {
    public static void main(String[] args) throws Exception {
        //1,获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2，获取数据
        DataStreamSource<String> text =  env.socketTextStream("localhost",9999);
        //3,transform调用算子
        //以下是, 读取每一行，按,分隔，返回(word,1)
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = value.toLowerCase().split(",");
                for (String token: tokens) {
                    if (token.length() > 0){//不是空字符串
                        collector.collect(new Tuple2<>(token,1));
                    }
                }
            }
        })
                .keyBy(0)// 按相同的单词分组
                .timeWindow(Time.seconds(5))// 每隔5s统计一次
                .sum(1)//
                .print();

        env.execute("StreamingWCJavaApp");

    }
}
