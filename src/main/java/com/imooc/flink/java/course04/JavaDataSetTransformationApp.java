package com.imooc.flink.java.course04;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class JavaDataSetTransformationApp {

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        mapFunction(env);
        filterFunction(env);
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
