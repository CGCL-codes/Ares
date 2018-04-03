package com.basic.benchmark.task;


import com.basic.benchmark.util.DataBaseUtil;
import java.sql.Timestamp;

/**
 * locate com.basic.benchmark.task
 * Created by 79875 on 2017/10/18.
 */
public class InsertSpoutLatencyTask implements Runnable{
    private Timestamp timestamp;
    private Double latencyTime;
    private int taskid;
    private boolean isGameSchedule;

    public InsertSpoutLatencyTask() {
    }

    public InsertSpoutLatencyTask(Timestamp timestamp, Double latencyTime, int taskid, boolean isGameSchedule) {
        this.timestamp = timestamp;
        this.latencyTime = latencyTime;
        this.taskid = taskid;
    }

    @Override
    public void run() {
        if(isGameSchedule)
            DataBaseUtil.insertAresSpoutLatency(timestamp, latencyTime, taskid);
        else
            DataBaseUtil.insertDefaultSpoutLatency(timestamp, latencyTime, taskid);

    }
}
