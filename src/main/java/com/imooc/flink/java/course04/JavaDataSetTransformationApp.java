package com.imooc.flink.java.course04;

import com.imooc.flink.scala.course04.DBUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class JavaDataSetTransformationApp {

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        mapFunction(env);
//        filterFunction(env);
        mapPartitionFunction(env);
    }
    //mapPartition
    private static void mapPartitionFunction(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<String>();
        for (int i = 1; i <= 100; i++) {
            list.add("student: "+ i);
        }
        env.fromCollection(list)
                //使用map 操作数据库,会产生很多数据库链接,与数据源的元素对应
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String input) throws Exception {
                        //每一个元素要存储到数据库,肯定需要先获取一个connection
                        String connection = DBUtils.getConection();
                        System.out.println("------map connection: "+ connection + " -------");
                        //TODO:保存到数据库,省略
                        DBUtils.returnConnection(connection);
                        return input;
                    }
                }).print();

        env.fromCollection(list)
                //使用mapPartition 操作数据库,数据库链接的产生
                .mapPartition(new MapPartitionFunction<String, String>() {
                    @Override
                    public void mapPartition(Iterable<String> inputs, Collector<String> out) throws Exception {
                        //每一个元素要存储到数据库,肯定需要先获取一个connection
                        String connection = DBUtils.getConection();
                        System.out.println("------mapPartition connection: "+ connection + " -------");
                        //TODO:保存到数据库,省略
                        DBUtils.returnConnection(connection);
                        for (String input: inputs ) {
                            out.collect(input);
                        }

                    }

                }).setParallelism(2)//控制mapPartition算子里有多少个Db connection
                 .print();
    }
    //filter
    private static void filterFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        env.fromCollection(list)
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer input) throws Exception {
                        return input + 1;  //每个元素+1
                    }
                }).filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer input) throws Exception {
                return input > 5;
            }
        }).print();
    }
    //map
    private static void mapFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        env.fromCollection(list)
                .map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer input) throws Exception {
                return input + 1;  //每个元素+1
            }
        }).print();
    }

}
