package com.imooc.flink.java.course05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据流与算子
 */
public class JavaDataStreamTransformationApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        filterFunction(env);
//        unionFunction(env);
        splitSelectFunction(env);
        env.execute("JavaDataStreamTransformationApp");
    }
    //split, select 的综合使用
    private static void splitSelectFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data1 =  env.addSource(new CustomNonParallelSourceFunction());
        //split数据 分解数据
        SplitStream<Long> splits =  data1.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                List<String> output = new ArrayList<>();
                if (value % 2 == 0){
                    output.add("even-java");
                }else{
                    output.add("odd-java");
                }
                return output;  //返回,可迭代的数据
            }
        });
        //select数据,输出split的odd数据
        splits.select("odd-java").print().setParallelism(1);
        //select数据,输出split的even数据
        splits.select("even-java").print().setParallelism(1);
    }

    //union
    private static void unionFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data1 =  env.addSource(new CustomNonParallelSourceFunction());
        DataStreamSource<Long> data2 =  env.addSource(new CustomNonParallelSourceFunction());
        DataStreamSource<Long> data3 =  env.addSource(new CustomNonParallelSourceFunction());
        data1.union(data2,data3).print().setParallelism(1);
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
