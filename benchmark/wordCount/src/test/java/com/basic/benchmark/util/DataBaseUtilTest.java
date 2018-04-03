package com.basic.benchmark.util;

import org.junit.Test;

import java.sql.Timestamp;

/**
 * locate com.basic.benchmark.util
 * Created by mastertj on 2018/4/3.
 */
public class DataBaseUtilTest {
    @Test
    public void insertAresWordCount() throws Exception {
        //DataBaseUtil.insertAresWordCount();
    }

    @Test
    public void insertAresSpoutCount() throws Exception {
        DataBaseUtil.insertAresSpoutCount(new Timestamp(System.currentTimeMillis()),111l,22);
    }

    @Test
    public void insertDefaultSpoutCount() throws Exception {
    }

    @Test
    public void insertAresSpoutLatency() throws Exception {
    }

    @Test
    public void insertDefaultSpoutLatency() throws Exception {
    }

}
