package com.basic.core;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * locate com.basic.core
 * Created by 79875 on 2017/9/29.
 */
public class TestConsole {
    @Test
    public void Test(){
        System.out.println("i am a console");
    }

    @Test
    public void Test2(){
        System.out.println("so you are a console?");
        List<Integer> list=new ArrayList<>();
        list.add(1);
        list.add(3);
        list.add(2);
        list.add(4);
        Collections.sort(list);
        System.out.println(list);
    }

//    @Test
//    public void Test3(){
//        Map<String,Double> map=new HashMap<>();
//        Double tanjie = map.get("tanjie");
//        System.out.println(String.valueOf(tanjie*1.0/2));
//    }
}
