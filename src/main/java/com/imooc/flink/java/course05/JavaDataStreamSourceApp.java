package com.imooc.flink.java.course05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * DataStream API编程
 * 测试socket : nc -L -p 9999
 */
public class JavaDataStreamSourceApp {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        socketFunction(env);
        //nonParallelSourceFunction(env);
//        ParallelSourceFunction(env);
        RichParallelSourceFunction(env);
        env.execute("JavaDataStreamSourceApp");
    }
    //支持并行的数据源
    private static void RichParallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new CustomRichParallelSourceFunction())
                .setParallelism(3);// 并行度 > 1 ,支持并行的数据源
        data.print();
    }    //支持并行的数据源
    private static void ParallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new CustomParallelSourceFunction())
                .setParallelism(3);// 并行度 > 1 ,支持并行的数据源
        data.print();
    }
    //不能并行的数据源
    private static void nonParallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new CustomNonParallelSourceFunction())
                .setParallelism(1);// 并行度 > 1 ,数据源输出的不正常
        data.print();
    }

    //侦听socket : localhost:9999
    private static void socketFunction(StreamExecutionEnvironment env) {
        DataStreamSource<String> data = env.socketTextStream("localhost",9999);
        data.print();
    }
}
