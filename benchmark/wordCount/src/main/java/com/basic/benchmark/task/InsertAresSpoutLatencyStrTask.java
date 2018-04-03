package com.basic.benchmark.task;


import com.basic.benchmark.util.DataBaseUtil;
import java.sql.Timestamp;

/**
 * locate com.basic.benchmark.task
 * Created by 79875 on 2017/10/18.
 */
public class InsertAresSpoutLatencyStrTask implements Runnable {
    private Timestamp timestamp;
    private String latencyTimeStr;
    private int taskid;
    public InsertAresSpoutLatencyStrTask() {
    }

    public InsertAresSpoutLatencyStrTask(Timestamp timestamp, String latencyTimeStr, int taskid) {
        this.timestamp = timestamp;
        this.latencyTimeStr = latencyTimeStr;
        this.taskid = taskid;
    }

    @Override
    public void run() {
        for(String latencyTime :latencyTimeStr.split(",")){
            if(!latencyTime.equals("")){
                DataBaseUtil.insertAresSpoutLatency(timestamp, Double.valueOf(latencyTime), taskid);
            }
        }
    }
}
