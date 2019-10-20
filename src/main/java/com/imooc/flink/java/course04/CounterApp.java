package com.imooc.flink.java.course04;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * 基于flink编程的计数器开发三步曲
 * 1,定义  val counter = new LongCounter()
 * 2,注册 getRuntimeContext.addAccumulator
 * 3,获取 jobResult.getAccumulatorResult
 */
public class CounterApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data =  env.fromElements("hadoop","spark","flink","pyspark","storm");

        //推荐:使用内置的计数器
        DataSet<String> info = data.map(new RichMapFunction<String, String>() {
            //1,定义计数器
            LongCounter counter = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2,注册计数器
                getRuntimeContext().addAccumulator("ele-counts-java",counter);
            }

            @Override
            public String map(String value) throws Exception {
                counter.add(1);//累计加一
                return value;
            }
        }).setParallelism(3);//使用map算子和计数器  ,进行计数时, 并行度 >1,满足需求

        info.print();
        JobExecutionResult jobResult = env.getLastJobExecutionResult();
        long count = jobResult.getAccumulatorResult("ele-counts-java");
        System.out.println("count : " + count);
    }
}
