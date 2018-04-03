package com.basic.benchmark.bench;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * locate com.basic.benchmark.bench
 * Created by mastertj on 2018/4/3.
 * 专门测试系统处理延迟(根据ACK机制来测试系统延迟)封装的Spout
 * 统计一秒钟内处理tuple的平均延迟
 */
public class LatecnySpout extends BaseRichSpout {
    private Timer timer;
    private int thisTaskId =0;
    protected SpoutOutputCollector spoutOutputCollector;
    private static final String LATENCYTIME_STREAM_ID="latencytimestream";
    private long totalDelay=0; //总和延迟 500个tuple
    private long tuplecount=0;
    private long startTimeMills;
    protected ConcurrentHashMap<UUID,Values> pending; //用来记录tuple的msgID，和tuple
    protected ConcurrentHashMap<UUID,Long> latencyHashMap; //用来统计tuple的延迟信息的HashMap
    private Logger LOG= LoggerFactory.getLogger(LatecnySpout.class);

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.spoutOutputCollector=collector;
        this.thisTaskId=topologyContext.getThisTaskId();
        this.pending=new ConcurrentHashMap<UUID, Values>();
        this.latencyHashMap=new ConcurrentHashMap<>();
        Timer timer = new Timer();

        //设置计时器没1s计算时间
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                if(startTimeMills!=0) {
                    double avgDelay= ((double) totalDelay / (double) tuplecount);
                    avgDelay=(double) Math.round(avgDelay*100)/100;
                    collector.emit(LATENCYTIME_STREAM_ID,new Values(thisTaskId,avgDelay,startTimeMills));
                    totalDelay=0;
                    tuplecount=0;
                }
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒
    }

    @Override
    public void nextTuple() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(LATENCYTIME_STREAM_ID,new Fields("taskid","latencytime","timeinfo"));
    }

    @Override
    public void ack(Object msgId) {
        startTimeMills=latencyHashMap.get(msgId);
        if(startTimeMills!=0){
            long endTime=System.currentTimeMillis();
            long latency=endTime-startTimeMills;
            totalDelay+=latency;
            tuplecount++;
            latencyHashMap.remove(msgId);
        }
        pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        this.spoutOutputCollector.emit(pending.get(msgId),msgId);
    }
}
