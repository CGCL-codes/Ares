package com.basic.benchmark;

import com.basic.util.DataBaseUtil;
import org.junit.Test;

import java.sql.Timestamp;

/**
 * locate com.basic.benchmark
 * Created by 79875 on 2017/10/5.
 */
public class ReportBoltTest {
    @Test
    public void execute() throws Exception {
        long startTime = System.nanoTime();
        String word="tanjie";
        Long count=10214L;

        //将最后结果插入到数据库中
        Timestamp timestamp=new Timestamp(System.currentTimeMillis());
        //logger.info("word:"+word+" tupplecount:"+count +" timeinfo"+timestamp);
        DataBaseUtil.insertAresWordCount(timestamp,word,count);
        //实际应用中，最后一个阶段，大部分应该是持久化到mysql，redis，es，solr或mongodb中
        long endTime = System.nanoTime();
        System.out.println(endTime-startTime);
    }

}
