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

/**
 * Created by dello on 2016/10/15.
 */
public class SentenceSpout extends BaseRichSpout {

    private static Logger logger= LoggerFactory.getLogger(SentenceSpout.class);

    public static final String WORDCOUNT_STREAM_ID="wordcountstream";
    public static final String ACKCOUNT_STREAM_ID="ackcountstream";
    public static final String LATENCYTIME_STREAM_ID="latencytimestream";

    private Timer timer;
    private int thisTaskId =0;
    private long ackcount=0; //记录单位时间ACK的元组数量

    private SpoutOutputCollector outputCollector;
    private int index=0;
    private ConcurrentHashMap<UUID,Values> pending; //用来记录tuple的msgID，和tuple
    private ConcurrentHashMap<UUID,Long> latencyHashMap; //用来统计tuple的延迟信息的HashMap

    private String[] sentences={
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "dont have acow man",
            "i dont think i like fleas",
            "i am very busy",
            "hello world i cant talk to you",
            "chinese is very nice i like it"
    };
    private boolean isSlowDown;
    private long waitTimeMills;

    private Random random=new Random();

    public SentenceSpout(long waitTimeMills) {
        this.waitTimeMills = waitTimeMills;
    }

    //初始化操作
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        logger.info("------------SentenceSpout open------------");
        this.outputCollector=spoutOutputCollector;

        this.thisTaskId=topologyContext.getThisTaskId();
        isSlowDown=AresUtils.isSlowDown();

        pending=new ConcurrentHashMap<UUID, Values>();
        latencyHashMap=new ConcurrentHashMap<>();

        timer=new Timer();
        //设置计时器没1s计算时间
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                //executor.execute(new WordCountTupleTask(new Timestamp(System.currentTimeMillis()),spoutcount));
                outputCollector.emit(ACKCOUNT_STREAM_ID,new Values(ackcount,System.currentTimeMillis(),thisTaskId));
                ackcount = 0;
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒
    }

    //向下游输出
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(WORDCOUNT_STREAM_ID,new Fields("word"));
        outputFieldsDeclarer.declareStream(ACKCOUNT_STREAM_ID,new Fields("tuplecount","timeinfo","taskid"));
        outputFieldsDeclarer.declareStream(LATENCYTIME_STREAM_ID,new Fields("latencytime","timeinfo","taskid"));
    }

    //核心逻辑
    public void nextTuple() {
        int sentencesNum= random.nextInt(sentences.length);
        String sentence=sentences[sentencesNum];
        String[] split = sentence.split(" ");
        String word=split[random.nextInt(split.length)];

        //Storm 的消息ack机制
        Values value = new Values(sentences[index]);
        UUID uuid=UUID.randomUUID();
        pending.put(uuid,value);
        latencyHashMap.put(uuid,System.currentTimeMillis());

        this.outputCollector.emit(value,uuid);
        //this.outputCollector.emit(value);
        
        index++;
        if(index>=sentences.length) index=0;

        if(isSlowDown){
            AresUtils.waitForTimeMillis(waitTimeMills);
        }
        outputCollector.emit(WORDCOUNT_STREAM_ID,new Values(word));

    }

    //Storm 的消息ack机制
    @Override
    public void ack(Object msgId) {
        ackcount++;
        pending.remove(msgId);

        //统计延迟时间
        Long startTime = latencyHashMap.get(msgId);
        Long endTime=System.currentTimeMillis();
        outputCollector.emit(LATENCYTIME_STREAM_ID,new Values(endTime-startTime,endTime,thisTaskId));
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
