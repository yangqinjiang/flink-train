package com.imooc.flink.java.course02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 使用Java API来开发Flink的批处理程序
 */
public class BatchWCJavaApp {
    public static void main(String[] args) throws Exception {
        // windows file system path
        //读取本地磁盘的文件
        /** hello.txt:
         * hello	world	weclome
         * hi	world	hello
         * weclome	keke
         */
        String filePath = "c:\\flink\\hello.txt";
        //1,获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2,读取数据
       DataSource<String> text = env.readTextFile(filePath);
       //3,transform 调用算子
        //以下是, 读取每一行，按\t分隔，返回(word,1)
        text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = value.toLowerCase().split("\\t");
                for (String token: tokens) {
                    if (token.length() > 0){//不是空字符串
                        collector.collect(new Tuple2<>(token,1));
                    }
                }
            }
        }).groupBy(0)//按单词分组,0 是指 (word,1)的word
                .sum(1)//统计相同单词的总数,1 是指 (word,1)的1
                .print();//在控制台打印
        /** output:
         * (weclome,2)
         * (world,2)
         * (hello,2)
         * (hi,1)
         * (keke,1)
         *
         */
    }
}
