package com.imooc.flink.java.course04;

/**
 * POJO 类,readCsvFile的使用
 */
public class Person {
    private String name;
    private int age;
    private String work;

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", work='" + work + '\'' +
                '}';
    }

    public Person() {
    }

    public Person(String name, int age, String work) {
        this.name = name;
        this.age = age;
        this.work = work;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void setWork(String work) {
        this.work = work;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public String getWork() {
        return work;
    }
}
