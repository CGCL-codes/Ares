package com.basic.benchmark;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

/**
 * locate com.basic.benchmark
 * Created by 79875 on 2017/10/5.
 */
public class WordCounterBoltTest {

    private Timer timer;
    private long tupplecount=0; //记录单位时间通过的元组数量
    private int thisTaskId =0;

    private Map<String, Long> counts = new HashMap<String, Long>();

    @Test
    public void execute() throws Exception {
        long startTime = System.nanoTime();
        tupplecount++;
        String word = "tanjie";
        if (!word.isEmpty()) {
            Long count = counts.get(word);
            if (count == null) {
                count = 0L;
            }
            count++;
            counts.put(word, count);
        }
        long nedTime = System.nanoTime();
        System.out.println(nedTime-startTime);
    }

}
