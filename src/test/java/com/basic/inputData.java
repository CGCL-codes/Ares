package com.basic;

import com.basic.util.DataBaseUtil;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;

/**
 * locate com.basic
 * Created by 79875 on 2017/10/21.
 */
public class inputData {

    @Test
    public void test() throws IOException {
        BufferedReader bufferedReader=new BufferedReader(new FileReader("F:\\2017研究生研究学习\\华科实验室学习\\学术\\林昌富Ares： a High Performance and Fault-tolerantDistributed Stream Processing System\\Ares 测试结果\\TPCSchemaTopology_1_1_9_1\\resourceSchedule\\lineitemlatency-15temp"));
//        BufferedReader bufferedReader=new BufferedReader(new FileReader("F:\\2017研究生研究学习\\华科实验室学习\\学术\\林昌富Ares： a High Performance and Fault-tolerantDistributed Stream Processing System\\Ares 测试结果\\TPCSchemaTopology_1_1_9_1\\resourceSchedule\\lineitemlatency-15temp"));
        String line=null;
        long throughput=0L;
        Timestamp currentTimestamp=null;
        while ((line = bufferedReader.readLine())!=null){
            String[] split = line.split("\t");
            throughput++;
            Timestamp timestamp = new Timestamp(Long.valueOf(split[1]));
            if(currentTimestamp==null||timestamp.getTime()!=currentTimestamp.getTime()){
                DataBaseUtil.insertTemp(throughput,currentTimestamp);
                currentTimestamp=timestamp;
                throughput=0;
            }
        }
    }
}

