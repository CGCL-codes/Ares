package com.basic.benchmark.bench;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.basic.benchmark.Constants.ACKCOUNT_STREAM_ID;
import static com.basic.benchmark.Constants.LATENCYTIME_STREAM_ID;

/**
 * locate com.basic.benchmark.spout
 * Created by 79875 on 2017/10/18.
 * 专门测试平均吞吐量和延迟的封装Spout 包括测试系统平均吞吐量和延迟的逻辑
 */
public class ThroughAvgLatencySpout extends BaseRichSpout{

    protected SpoutOutputCollector outputCollector;
    protected ConcurrentHashMap<UUID,Values> pending; //用来记录tuple的msgID，和tuple
    protected ConcurrentHashMap<UUID,Long> latencyHashMap; //用来统计tuple的延迟信息的HashMap

    private int thisTaskId =0;
    private long ackcount=0; //记录单位时间ACK的元组数量
    private LatencyModel latencyModel=new LatencyModel();


    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector=spoutOutputCollector;
        this.thisTaskId=topologyContext.getThisTaskId();
        this.pending=new ConcurrentHashMap<UUID, Values>();
        this.latencyHashMap=new ConcurrentHashMap<>();
        Timer latencyTimer = new Timer();
        //设置计时器没1s计算时间
        latencyTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                //executor.execute(new WordCountTupleTask(new Timestamp(System.currentTimeMillis()),spoutcount));
                outputCollector.emit(ACKCOUNT_STREAM_ID,new Values(ackcount,System.currentTimeMillis(),thisTaskId));
                ackcount = 0;
                outputCollector.emit(LATENCYTIME_STREAM_ID,new Values(latencyModel.computeAvglatency(),System.currentTimeMillis(),thisTaskId));
            }
        }, 10,1000);// 设定指定的时间time,此处为1000毫秒
    }

    @Override
    public void nextTuple() {

    }

    @Override
    public void ack(Object msgId) {
        Long endTime = System.currentTimeMillis();
        ackcount++;

        //统计延迟时间
        pending.remove(msgId);
        Long startTime = latencyHashMap.get(msgId);
        if(startTime!=0) {
            latencyModel.computeLatency(endTime - startTime);
            latencyHashMap.remove(msgId);
        }
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

class LatencyModel implements Serializable {
    private Long totalLatency=0L;
    private Long totalTuple=0L;
    private int lock=0;

    public Long getTotalLatency() {
        return totalLatency;
    }

    public void setTotalLatency(Long totalLatency) {
        this.totalLatency = totalLatency;
    }

    public Long getTotalTuple() {
        return totalTuple;
    }

    public void setTotalTuple(Long totalTuple) {
        this.totalTuple = totalTuple;
    }

     public void computeLatency(Long latency){
        synchronized(LatencyModel.class){
            totalTuple++;
            totalLatency+=latency;
        }
    }

    public long computeAvglatency(){
        synchronized(LatencyModel.class) {
            if (totalTuple == 0)
                return 0;
            return totalLatency / totalTuple;
        }
    }

}
