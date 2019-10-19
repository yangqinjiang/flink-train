package com.imooc.flink.java.course04;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据源
 */
public class JavaDataSetDataSourceApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //formCollection(env);
        readTextFile(env);
        readTextDir(env);
    }
    //从本地文件夹读取数据，并打印
    private static void readTextDir(ExecutionEnvironment env) throws Exception{
        String filePath = "c:\\flink\\input";
        env.readTextFile(filePath).print();
    }
    //从本地文件读取数据，并打印
    private static void readTextFile(ExecutionEnvironment env) throws Exception{
        String filePath = "c:\\flink\\hello.txt";
        env.readTextFile(filePath).print();
    }

    //从本地集合读取数据，并打印
    private static void formCollection(ExecutionEnvironment env) throws Exception{
        List<Integer> list = new ArrayList<Integer>();
        for(int i=1;i<=10;i++){
            list.add(i);
        }
        env.fromCollection(list).print();
    }

}
