package com.basic.core;

import com.basic.core.util.AresUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * locate com.basic.core
 * Created by 79875 on 2017/10/10.
 */
public class TestHashMap {
    @Test
    public void Test(){
        Map<String,String> map=new HashMap<>();
        map.put("tanjie","123");
        System.out.println(map);
        fun1(map);
        System.out.println(map);
    }

    private void fun1(Map<String, String> map) {
        //map=new HashMap<>();
        HashMap<String, List<String>> stringListHashMap = AresUtils.reverseMap(map);
        map.clear();
        System.out.println(stringListHashMap);
        map.put("abc","123");
    }

    @Test
    public void test2(){
        Double a=0.0;
        a+=5;
        System.out.println(a);
    }
}
