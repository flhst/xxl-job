package com.xml.job.core.util;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.gson.reflect.TypeToken;
import com.xxl.job.core.util.GsonTool;

/**
 * @author hst
 * @create 2024-12-16 14:43
 * @Description:
 */
public class GsonToolTest {

    @Test
    public void fromJsonListTest() {
        String json = "[{\"name\":\"John\", \"age\":30}, {\"name\":\"Anna\", \"age\":22}]";
        TypeToken<List<Person>> typeToken = new TypeToken<List<Person>>() {};
        List<Person> personList = GsonTool.fromJsonList(json, typeToken);
        personList.forEach(System.out::println);
    }


}


class Person {
    private String name;
    private int age;


    public Person() {
        super();
    }

    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
