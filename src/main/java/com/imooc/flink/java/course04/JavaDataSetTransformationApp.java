package com.imooc.flink.java.course04;

import com.imooc.flink.scala.course04.DBUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 参考文档
 * https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/batch/dataset_transformations.html#join
 */
public class JavaDataSetTransformationApp {

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        mapFunction(env);
//        filterFunction(env);
//        mapPartitionFunction(env);
//        firstFunction(env);
//        flatMapFunction(env);
//        distinctFunction(env);
//        joinFunction(env);
//        leftOuterJoinFunction(env);
//        rightOuterJoinFunction(env);
//        fullOuterJoinFunction(env);
        crossFunction(env);
    }
    public static void  crossFunction(ExecutionEnvironment env) throws Exception{
        List<String> info1 = new ArrayList<>();
        info1.add("曼联");
        info1.add("曼城");

        List<Integer> info2 = new ArrayList<>();
        info2.add(3);
        info2.add(1);
        info2.add(0);

        DataSource<String> data1 = env.fromCollection(info1);
        DataSource<Integer> data2 = env.fromCollection(info2);
        data1.cross(data2).print();
        /** output:
         * (曼联,3)
         * (曼联,1)
         * (曼联,0)
         * (曼城,3)
         * (曼城,1)
         * (曼城,0)
         */
    }
    //fullOuterJoin 全匹配,右表 ,左表可能有null值
    private static void fullOuterJoinFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer,String>> info1 = new ArrayList<>();
        info1.add(new Tuple2<>(1,"PK哥"));
        info1.add(new Tuple2<>(2,"J哥"));
        info1.add(new Tuple2<>(3,"小队长"));
        info1.add(new Tuple2<>(4,"猪头呼"));

        List<Tuple2<Integer,String>> info2 = new ArrayList<>();
        info2.add(new Tuple2<>(1,"北京"));
        info2.add(new Tuple2<>(2,"上海"));
        info2.add(new Tuple2<>(3,"成都"));
        info2.add(new Tuple2<>(5,"杭州"));
        DataSource<Tuple2<Integer,String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer,String>> data2 = env.fromCollection(info2);

        data1.fullOuterJoin(data2)
                .where(0)//关联
                .equalTo(0)//关联
                //返回tuple3
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first == null){
                            return new Tuple3<>(second.f0,"-",second.f1);
                        }else if(second == null){
                            return new Tuple3<>(first.f0,first.f1,"-");
                        }else{
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }

                    }
                }).print();
        /** output:
         *(3,小队长,成都)
         * (1,PK哥,北京)
         * (5,-,杭州)
         * (2,J哥,上海)
         * (4,猪头呼,-)
         */
    }
    //rightOuterJoin 右匹配,以右表为基准,左表可能有null值
    private static void rightOuterJoinFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer,String>> info1 = new ArrayList<>();
        info1.add(new Tuple2<>(1,"PK哥"));
        info1.add(new Tuple2<>(2,"J哥"));
        info1.add(new Tuple2<>(3,"小队长"));
        info1.add(new Tuple2<>(4,"猪头呼"));

        List<Tuple2<Integer,String>> info2 = new ArrayList<>();
        info2.add(new Tuple2<>(1,"北京"));
        info2.add(new Tuple2<>(2,"上海"));
        info2.add(new Tuple2<>(3,"成都"));
        info2.add(new Tuple2<>(5,"杭州"));
        DataSource<Tuple2<Integer,String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer,String>> data2 = env.fromCollection(info2);

        data1.rightOuterJoin(data2)
                .where(0)//关联
                .equalTo(0)//关联
                //返回tuple3
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first != null){
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }else{
                            return new Tuple3<>(second.f0,"-",second.f1);
                        }

                    }
                }).print();
        /** output:
         *(3,小队长,成都)
         * (1,PK哥,北京)
         * (5,-,杭州)
         * (2,J哥,上海)
         */
    }
    //leftOuterJoin 左匹配,以左表为基准,右表可能有null值
    private static void leftOuterJoinFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer,String>> info1 = new ArrayList<>();
        info1.add(new Tuple2<>(1,"PK哥"));
        info1.add(new Tuple2<>(2,"J哥"));
        info1.add(new Tuple2<>(3,"小队长"));
        info1.add(new Tuple2<>(4,"猪头呼"));

        List<Tuple2<Integer,String>> info2 = new ArrayList<>();
        info2.add(new Tuple2<>(1,"北京"));
        info2.add(new Tuple2<>(2,"上海"));
        info2.add(new Tuple2<>(3,"成都"));
        info2.add(new Tuple2<>(5,"杭州"));
        DataSource<Tuple2<Integer,String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer,String>> data2 = env.fromCollection(info2);

        data1.leftOuterJoin(data2)
                .where(0)//关联
                .equalTo(0)//关联
                //返回tuple3
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (second != null){
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }else{
                            return new Tuple3<>(first.f0,first.f1,"-");
                        }

                    }
                }).print();
        /** output:
         *(3,小队长,成都)
         * (1,PK哥,北京)
         * (2,J哥,上海)
         * (4,猪头呼,-)
         */
    }
    //join 全匹配
    private static void joinFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer,String>> info1 = new ArrayList<>();
        info1.add(new Tuple2<>(1,"PK哥"));
        info1.add(new Tuple2<>(2,"J哥"));
        info1.add(new Tuple2<>(3,"小队长"));
        info1.add(new Tuple2<>(4,"猪头呼"));

        List<Tuple2<Integer,String>> info2 = new ArrayList<>();
        info2.add(new Tuple2<>(1,"北京"));
        info2.add(new Tuple2<>(2,"上海"));
        info2.add(new Tuple2<>(3,"成都"));
        info2.add(new Tuple2<>(5,"杭州"));
        DataSource<Tuple2<Integer,String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer,String>> data2 = env.fromCollection(info2);

        data1.join(data2)
                .where(0)//关联
                .equalTo(0)//关联
                //返回tuple3
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String,String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                return new Tuple3<>(first.f0,first.f1,second.f1);
            }
        }).print();
        /** output:
         *(3,小队长,成都)
         * (1,PK哥,北京)
         * (2,J哥,上海)
         */
    }
    //distinctFunction,去重
    private static void distinctFunction(ExecutionEnvironment env) throws Exception{
        List<String> info = new ArrayList<>();
        info.add("hadoop,spark");
        info.add("hadoop,flink");
        info.add("flink,flink");
        DataSource<String> data =  env.fromCollection(info);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] tokens = value.split(",");
                for (String token : tokens){
                    out.collect(token);
                }
            }
        }).distinct().print();
        /** output:
         * hadoop
         * flink
         * spark
         */

    }
    //flatMapFunction,统计单词数量
    private static void flatMapFunction(ExecutionEnvironment env) throws Exception{
        List<String> info = new ArrayList<>();
        info.add("hadoop,spark");
        info.add("hadoop,flink");
        info.add("flink,flink");
        DataSource<String> data =  env.fromCollection(info);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] tokens = value.split(",");
                for (String token : tokens){
                    out.collect(token);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {//注意输出与输出的数据类型
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value,1);
            }
        }).groupBy(0).sum(1).print();
        /** output:
         * (hadoop,2)
         * (flink,3)
         * (spark,1)
         */

    }
    //first
    private static void firstFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer,String>> info = new ArrayList<>();
        info.add(new Tuple2<>(1,"Hadoop"));
        info.add(new Tuple2<>(1,"Spark"));
        info.add(new Tuple2<>(1,"Flink"));
        info.add(new Tuple2<>(2,"Java"));
        info.add(new Tuple2<>(2,"Spring Boot"));
        info.add(new Tuple2<>(3,"Linux"));
        info.add(new Tuple2<>(4,"VUE.js"));

        DataSource<Tuple2<Integer,String>> data =  env.fromCollection(info);
        data.first(3).print();
        /** output:
         * (1,Hadoop)
         * (1,Spart)
         * (1,Flink)
         */
        data.groupBy(0).first(2).print();
        /** output:
         * (3,Linux)
         * (1,Hadoop)
         * (1,Spart)
         * (2,Java)
         * (2,String Boot)
         * (4,Vue.js)
         */
        data.groupBy(0)
                .sortGroup(1, Order.DESCENDING)//按字母降序
                .first(2).print();

        /** output:
         * (3,Linux)
         * (1,Spart)
         * (1,Hadoop)
         * (2,String Boot)
         * (2,Java)
         * (4,Vue.js)
         */
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
