package com.basic.benchmark;

import com.basic.core.util.AresUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by dello on 2016/10/15.
 */
public class SentenceSpout extends BaseRichSpout {

    private static Logger logger= LoggerFactory.getLogger(SentenceSpout.class);

    public static final String WORDCOUNT_STREAM_ID="wordcountstream";
    public static final String ACKCOUNT_STREAM_ID="ackcountstream";
    public static final String LATENCYTIME_STREAM_ID="latencytimestream";

    private Timer latencyTimer;
    private Timer throughputTimer;
    private int thisTaskId =0;
    private long ackcount=0; //记录单位时间ACK的元组数量
    private List<Long> latencyList=new ArrayList<>();//用来收集 Latency List 链表

    private SpoutOutputCollector outputCollector;
    private int index=0;
    private ConcurrentHashMap<UUID,Values> pending; //用来记录tuple的msgID，和tuple
    private ConcurrentHashMap<UUID,Long> latencyHashMap; //用来统计tuple的延迟信息的HashMap

    private boolean isSlowDown;
    private long waitTimeMills;
    private Lock lock=new ReentrantLock();//用来控制延迟输出的Lock
    private Random random=new Random();

    public SentenceSpout(long waitTimeMills) {
        this.waitTimeMills = waitTimeMills;
    }

    private String randomWords(int wordLength){
        char[] chars=new char[wordLength];
        for(int i=0;i<wordLength;i++){
            char c=(char)('A'+Math.random()*('Z'-'A'+1));
            chars[i]=c;
        }
        return new String(chars);
    }

    //初始化操作
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        logger.info("------------SentenceSpout open------------");
        this.outputCollector=spoutOutputCollector;

        this.thisTaskId=topologyContext.getThisTaskId();
        isSlowDown=AresUtils.isSlowDown();

        pending=new ConcurrentHashMap<UUID, Values>();
        latencyHashMap=new ConcurrentHashMap<>();

        throughputTimer=new Timer();
        //设置计时器没1s计算时间
        throughputTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                //executor.execute(new WordCountTupleTask(new Timestamp(System.currentTimeMillis()),spoutcount));
                outputCollector.emit(ACKCOUNT_STREAM_ID,new Values(ackcount,System.currentTimeMillis(),thisTaskId));
                ackcount = 0;
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒

        latencyTimer=new Timer();
        latencyTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                Iterator<Long> iterator = latencyList.iterator();
                for(int i=0;i<latencyList.size();i++){
                    Long latencyTime = iterator.next();
                    outputCollector.emit(LATENCYTIME_STREAM_ID, new Values(latencyTime, System.currentTimeMillis(), thisTaskId));
                    iterator.remove();
                }
            }
        },1,2600);
    }

    //向下游输出
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(WORDCOUNT_STREAM_ID,new Fields("word"));
        outputFieldsDeclarer.declareStream(ACKCOUNT_STREAM_ID,new Fields("tuplecount","timeinfo","taskid"));
        outputFieldsDeclarer.declareStream(LATENCYTIME_STREAM_ID,new Fields("latencytime","timeinfo","taskid"));
    }

    //核心逻辑
    public void nextTuple() {
        if(isSlowDown){
            AresUtils.waitForTimeMillis(waitTimeMills);
        }

        String word=randomWords(5);
        //Storm 的消息ack机制
        Values value = new Values(word);
        UUID uuid=UUID.randomUUID();
        pending.put(uuid,value);
        latencyHashMap.put(uuid,System.currentTimeMillis());

        outputCollector.emit(WORDCOUNT_STREAM_ID,value,uuid);
    }

    //Storm 的消息ack机制
    @Override
    public void ack(Object msgId) {
        ackcount++;
        pending.remove(msgId);

        //统计延迟时间
        Long startTime = latencyHashMap.get(msgId);
        Long endTime=System.currentTimeMillis();
        latencyList.add(endTime-startTime);
        latencyHashMap.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        this.outputCollector.emit(pending.get(msgId),msgId);
    }

    @Override
    public void close() {
    }
}

//class LatencyModel implements Serializable{
//    private Long totalLatency=0L;
//    private Long totalTuple=0L;
//
//    public Long getTotalLatency() {
//        return totalLatency;
//    }
//
//    public void setTotalLatency(Long totalLatency) {
//        this.totalLatency = totalLatency;
//    }
//
//    public Long getTotalTuple() {
//        return totalTuple;
//    }
//
//    public void setTotalTuple(Long totalTuple) {
//        this.totalTuple = totalTuple;
//    }
//
//    synchronized public void computeLatency(Long latency){
//        totalTuple++;
//        totalLatency+=latency;
//    }
//
//    public long computeAvglatency(){
//        if(totalTuple==0)
//            return 0;
//        return totalLatency/totalTuple;
//    }
//
//}
