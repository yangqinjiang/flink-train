package com.imooc.flink.java.course04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

public class JavaDataSetSinkApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> info = new ArrayList<>();
        for(int i=1;i<=100;i++){
            info.add(i);
        }
        DataSource<Integer> data = env.fromCollection(info);//数据源
        String filePath = "c:\\flink\\output\\sink-out-java";
        //设置参数, FileSystem.WriteMode.OVERWRITE ,已存在filePath,则覆盖
        //如果 setParallelism 设置并行度 > 1,则slin-out是文件夹,里面包括与并行度数量相同的文件
        // , setParallelism(1)则输出是单个文件
        data.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE).setParallelism(2);
        env.execute("JavaDataSetSinkApp");
    }
}
