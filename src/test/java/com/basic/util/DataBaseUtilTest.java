package com.basic.util;

import org.junit.Test;

import java.sql.Timestamp;

/**
 * locate com.basic.util
 * Created by 79875 on 2017/10/19.
 */
public class DataBaseUtilTest {
    @Test
    public void insertAresSpoutCount() throws Exception {
        DataBaseUtil.insertAresSpoutCount(new Timestamp(System.currentTimeMillis()), 100L, 4);
    }

}
