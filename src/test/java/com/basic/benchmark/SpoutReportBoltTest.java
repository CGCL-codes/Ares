package com.basic.benchmark;

import com.basic.util.DataBaseUtil;
import org.junit.Test;

import java.sql.Timestamp;

/**
 * locate com.basic.benchmark
 * Created by 79875 on 2017/10/5.
 */
public class SpoutReportBoltTest {
    @Test
    public void execute() throws Exception {
        long startTime = System.nanoTime();
        Long currentTimeMills=System.currentTimeMillis();
        Long tupplecount=1024L;
        int taskid=1;

        //将最后结果插入到数据库中
        Timestamp timestamp=new Timestamp(currentTimeMills);
        DataBaseUtil.insertAresSpoutCount(timestamp,tupplecount,taskid);
        long endTime = System.nanoTime();
        System.out.println(endTime-startTime);
        //实际应用中，最后一个阶段，大部分应该是持久化到mysql，redis，es，solr或mongodb中
    }

}
