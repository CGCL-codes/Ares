package com.basic.util;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by 79875 on 2017/1/11.
 */
public class PropertiesUtil {

    public static Properties pro;

    public static void init(String fileName) {
        pro = new Properties();
        try {
            InputStream in = Object.class.getResourceAsStream(fileName);
            pro.load(in);
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getProperties(String name){
        return pro.getProperty(name);
    }

    public static void setProperties(String name ,String value){
        pro.setProperty(name, value);
    }
}
