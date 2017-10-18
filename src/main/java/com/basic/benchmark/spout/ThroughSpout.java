package com.basic.benchmark.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * locate com.basic.benchmark.spout
 * Created by 79875 on 2017/10/18.
 * 专门测试吞吐量的封装Spout 包括测试系统吞吐量的逻辑
 */
public class ThroughSpout extends BaseRichSpout {
    private static final String ACKCOUNT_STREAM_ID="ackcountstream";
    protected SpoutOutputCollector outputCollector;
    ConcurrentHashMap<UUID,Values> pending; //用来记录tuple的msgID，和tuple

    private int thisTaskId =0;
    private long ackcount=0; //记录单位时间ACK的元组数量

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector=spoutOutputCollector;
        this.thisTaskId=topologyContext.getThisTaskId();
        this.pending=new ConcurrentHashMap<UUID, Values>();
        Timer throughputTimer = new Timer();
        //设置计时器没1s计算时间
        throughputTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                outputCollector.emit(ACKCOUNT_STREAM_ID,new Values(ackcount,System.currentTimeMillis(),thisTaskId));
                ackcount = 0;
            }
        }, 10,1000);// 设定指定的时间time,此处为1000毫秒
    }

    @Override
    public void nextTuple() {

    }

    @Override
    public void ack(Object msgId) {
        ackcount++;
        pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        this.outputCollector.emit(pending.get(msgId),msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(ACKCOUNT_STREAM_ID,new Fields("tuplecount","timeinfo","taskid"));
    }
}
