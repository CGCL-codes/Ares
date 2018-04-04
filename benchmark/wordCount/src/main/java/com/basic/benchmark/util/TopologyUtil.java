package com.basic.benchmark.util;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * locate com.basic.util
 * Created by 79875 on 2017/10/18.
 */
public class TopologyUtil {
    public static boolean isSlowDown(){
        String filepath="/storm/apache-storm-1.2.1/slowdown";
        BufferedReader bufferedReader= null;
        boolean isSlowDown = false;
        try {
            bufferedReader = new BufferedReader(new FileReader(filepath));
            isSlowDown = Boolean.valueOf(bufferedReader.readLine());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return isSlowDown;
    }
}
