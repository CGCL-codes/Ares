package com.basic.core.util;

import com.basic.util.PropertiesUtil;
import org.apache.storm.scheduler.ExecutorDetails;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * locate com.basic.core.util
 * Created by 79875 on 2017/9/30.
 */
public class AresUtilsTest {
    @Test
    public void initializeD() throws Exception {
        PropertiesUtil.init("/componentcost.properties");
        Map<ExecutorDetails, Double> q = new HashMap<ExecutorDetails, Double>();
        String properties = PropertiesUtil.getProperties("report-bolt");
        System.out.println(properties);
        Double.valueOf(PropertiesUtil.getProperties("report-bolt"));

        PropertiesUtil.initAbsolutePath("D:\\ProgramProjects\\IntelJIDEAProject\\IntelliJIDEA15.0.2\\BigDataProject\\StormProject\\aresStorm\\src\\main\\resources\\componentcost.properties");
        String properties2 = PropertiesUtil.getProperties("report-bolt");
        System.out.println(properties2);
        Double.valueOf(PropertiesUtil.getProperties("report-bolt"));
    }

    @Test
    public void Test(){
        int a=1;
        int b=3;
        double temp=a/b;
        System.out.println(temp);
    }

    @Test
    public void initializeDa(){
        PropertiesUtil.init("/nodetransferpair.properties");
        String properties = PropertiesUtil.getProperties("root9,root9");
        System.out.println(properties);
    }

}
