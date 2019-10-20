package com.imooc.flink.java.course05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JavaDataStreamTransformationApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        filterFunction(env);
        env.execute("JavaDataStreamTransformationApp");
    }

    private static void filterFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data =  env.addSource(new CustomNonParallelSourceFunction());
        data.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("java received: " + value);
                return value;
            }
        }).filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0; //只要偶数
            }
        }).print().setParallelism(1);
    }
}
