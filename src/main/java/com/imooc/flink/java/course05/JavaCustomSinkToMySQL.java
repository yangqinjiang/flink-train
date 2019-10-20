package com.imooc.flink.java.course05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从socket接收数据, 经过flink处理后, sink到MySQL数据库
 * 测试socket : nc -L -p 9999
 */

public class JavaCustomSinkToMySQL {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String>source = env.socketTextStream("localhost",9999);
        //map返回 student stream
        SingleOutputStreamOperator<Student> studentStream =  source.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {

                //map,接收字符串,返回student对象
                String[] splits = value.split(",");
                if (splits.length != 3){
                    //传入的数据不正常,直接返回null
                    return null;
                }
                Student student = new Student();
                student.setId(Integer.parseInt(splits[0]));
                student.setName(splits[1]);
                student.setAge(Integer.parseInt(splits[2]));
                System.out.println("map: " + value + " , student: " + student);
                return student;
            }
        }).filter(new FilterFunction<Student>() {//过滤null值
            @Override
            public boolean filter(Student value) throws Exception {
                //过滤null值
                return value != null;
            }
        });
        studentStream.addSink(new SinkToMySQL());
        env.execute("JavaDataStreamTransformationApp");
    }
}
