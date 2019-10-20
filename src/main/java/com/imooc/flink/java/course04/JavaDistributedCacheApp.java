package com.imooc.flink.java.course04;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.List;

public class JavaDistributedCacheApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String filePath = "c:\\flink\\hello.txt";
        //1,注册一个本地/HDFS文件
        env.registerCachedFile(filePath,"pk-java-dc");

        DataSource<String> data =  env.fromElements("hadoop","spark","flink","pyspark","storm");

        DataSet<String> info = data.map(new RichMapFunction<String, String>() {

            //2,在open方法中,获取到分布式缓存的内容即可
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //读取分布式缓存的文件
                File dcFile = getRuntimeContext().getDistributedCache().getFile("pk-java-dc");
                List<String> lines = FileUtils.readLines(dcFile);
                for (String line: lines ) {
                    System.out.println(line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).setParallelism(1);

        info.print();
    }
}
