package com.basic.benchmark.task;

import com.basic.util.DataBaseUtil;

import java.sql.Timestamp;

/**
 * locate com.basic.benchmark.task
 * Created by 79875 on 2017/10/18.
 */
public class InsertAresSpoutLatencyTask implements Runnable{
    private Timestamp timestamp;
    private Long latencyTime;
    private int taskid;
    public InsertAresSpoutLatencyTask() {
    }

    public InsertAresSpoutLatencyTask(Timestamp timestamp, Long latencyTime, int taskid) {
        this.timestamp = timestamp;
        this.latencyTime = latencyTime;
        this.taskid = taskid;
    }

    @Override
    public void run() {
        DataBaseUtil.insertAresSpoutLatency(timestamp, Long.valueOf(latencyTime), taskid);
    }
}
