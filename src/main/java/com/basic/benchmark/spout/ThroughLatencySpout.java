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
 * 专门测试吞吐量和延迟的封装Spout 包括测试系统吞吐量和延迟的逻辑
 */
public class ThroughLatencySpout extends BaseRichSpout {
    private static final String ACKCOUNT_STREAM_ID="ackcountstream";
    private static final String LATENCYTIME_STREAM_ID="latencytimestream";
    protected SpoutOutputCollector outputCollector;
    ConcurrentHashMap<UUID,Values> pending; //用来记录tuple的msgID，和tuple
    ConcurrentHashMap<UUID,Long> latencyHashMap; //用来统计tuple的延迟信息的HashMap

    private int thisTaskId =0;
    private long ackcount=0; //记录单位时间ACK的元组数量
    private Queue<Long> latencyQueue=new ArrayDeque<>();//用来收集 Latency List 链表

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector=spoutOutputCollector;
        this.thisTaskId=topologyContext.getThisTaskId();
        this.pending=new ConcurrentHashMap<UUID, Values>();
        this.latencyHashMap=new ConcurrentHashMap<>();
        Timer throughputTimer = new Timer();
        //设置计时器没1s计算时间
        throughputTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                outputCollector.emit(ACKCOUNT_STREAM_ID,new Values(ackcount,System.currentTimeMillis(),thisTaskId));
                ackcount = 0;
            }
        }, 10,1000);// 设定指定的时间time,此处为1000毫秒
        Timer latencyTimer = new Timer();
        latencyTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                for(int i=0;i<600;i++){
                    if(latencyQueue.isEmpty()){
                        break;
                    }
                    Long latencyTime = latencyQueue.poll();
                    outputCollector.emit(LATENCYTIME_STREAM_ID, new Values(latencyTime, System.currentTimeMillis(), thisTaskId));
                }
            }
        },10,600);
    }

    @Override
    public void nextTuple() {

    }

    @Override
    public void ack(Object msgId) {
        ackcount++;
        Long endTime = System.currentTimeMillis();
        //统计延迟时间
        Long startTime = latencyHashMap.get(msgId);
        if(startTime!=0){
            latencyQueue.add(endTime - startTime);
            latencyHashMap.remove(msgId);
        }
        pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        this.outputCollector.emit(pending.get(msgId),msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(ACKCOUNT_STREAM_ID,new Fields("tuplecount","timeinfo","taskid"));
        outputFieldsDeclarer.declareStream(LATENCYTIME_STREAM_ID,new Fields("latencytime","timeinfo","taskid"));
    }
}
