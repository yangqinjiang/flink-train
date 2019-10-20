package com.imooc.flink.java.course05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 自定义Mysql sink
 */
public class SinkToMySQL extends RichSinkFunction<Student> {
    Connection connection;
    PreparedStatement pstmt;

    private Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://localhost:3306/imooc_flink";
            conn = DriverManager.getConnection(url,"root","");
        }catch (ClassNotFoundException e){
            System.err.println("ClassNotFoundException:" +e.getLocalizedMessage());
        }catch (SQLException e){
            System.err.println("SQLException:" +e.getLocalizedMessage());
        }
        return  conn;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("sink to mysql,初始化数据库链接,");
        //初始化数据库链接,
        connection = getConnection();
        if( connection != null){
            String sql = "insert into student (id,name,age) values (?,?,?)";
            pstmt = connection.prepareStatement(sql);
        }

    }


    //每条记录插入时,调用一次invoke
    @Override
    public void invoke(Student value, Context context) throws Exception {
        System.out.println("invoke,sink to mysql, student: " + value);
        if( null == pstmt || null == value ){
            return;
        }
        pstmt.setInt(1,value.getId());
        pstmt.setString(2,value.getName());
        pstmt.setInt(3,value.getAge());

        pstmt.executeUpdate();
    }
}
