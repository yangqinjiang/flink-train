package com.imooc.flink.java.course04;

import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.configuration.Configuration;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据源
 */
public class JavaDataSetDataSourceApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //formCollection(env);
        //readTextFile(env);
        //readTextDir(env);
//        csvFile(env);
        //readRecursiveFiles(env);
        readCompressionFiles(env);
    }
    //读取压缩文件
    private static void readCompressionFiles(ExecutionEnvironment env) throws Exception{
        String filePath = "c:\\flink\\compression";
        env.readTextFile(filePath).print();
    }
    // 从递归文件夹的内容创建dataset之java实现
    private static void readRecursiveFiles(ExecutionEnvironment env) throws Exception {
        String filePath = "c:\\flink\\nested";
        //配置 递归文件夹
        Configuration conf  = new Configuration();
        conf.setBoolean("recursive.file.enumeration",true);
        env.readTextFile(filePath).withParameters(conf).print();
    }

    //从csv文件读取数据,并打印
    private static void csvFile(ExecutionEnvironment env) throws Exception {
        /** 文件内容示例:
         * name,age,job
         * Jorge,30,Developer
         * Bob,32,Developer
         */
        String filePath = "c:\\flink\\people.csv";
        //指定数据类型或者tuple元组
        //并且不要解析第一行数据
        //输出三列数据
        env.readCsvFile(filePath).ignoreFirstLine().types(String.class,Integer.class,String.class).print();
        //输出三列数据,includedFields对应csv文件的顺序号,如0->name,  1->age,  2->job
        /** 文件内容示例:
         * name,age,job
         * Jorge,30,Developer
         * Bob,32,Developer
         */

        env.readCsvFile(filePath)
                .ignoreFirstLine()
                .types(String.class,Integer.class,String.class)//输出所有
                .print();
        //输出csv文件的两列数据 name,job , includedFields = true,false,true
        env.readCsvFile(filePath)
                .ignoreFirstLine()
                .includeFields(true,false,true)//忽略age列
                .types(String.class,String.class)//对应上一行代码的true/false
                .print();
        //POJO,使用java的类,输出name,age,work
        env.readCsvFile(filePath).ignoreFirstLine()
                .pojoType(Person.class,"name","age","work").print();
        //POJO,使用java的类,输出name,age
        env.readCsvFile(filePath).ignoreFirstLine()
                .pojoType(Person.class,"name","age").print();
        /** output:
         * Person{name='Bob', age=32, work=''}
         * Person{name='Jorge', age=30, work=''}
         */
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
