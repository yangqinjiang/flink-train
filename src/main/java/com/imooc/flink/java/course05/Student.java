package com.imooc.flink.java.course05;

/**
 * 学生类,对应数据库
 *
 */
/* MySQL数据库及表
create database imooc_flink;
create table student(
    id int(11) NOT NULL AUTO_INCREMENT,
    name varchar(25),
    age int(10),
    primary key (id)
);
 */
public class Student {
    private int id;
    private String name;
    private int age;

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }
}
