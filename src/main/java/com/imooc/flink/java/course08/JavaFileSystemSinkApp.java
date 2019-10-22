package com.imooc.flink.java.course08;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
/**
 * 将结果写出到文件系统
 */
public class JavaFileSystemSinkApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.socketTextStream("localhost",9999);
        data.print().setParallelism(1);
        String filePath = "c:\\flink\\output\\filesystem_sink\\java";
        BucketingSink<String> sink = new BucketingSink<String>(filePath);
        //文件夹的命名规范
        sink.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd--HHmm"));

        sink.setWriter(new StringWriter<String>());
        //sink.setBatchSize(1024*1024*400); // 400MB
        //sink.setBatchRolloverInterval(20*60*1000); // 20 mins
        //每隔多少秒,写一次??
        sink.setBatchRolloverInterval(2000); // 20s
        data.addSink(sink);
        env.execute("JavaFileSystemSinkApp");
    }
}
