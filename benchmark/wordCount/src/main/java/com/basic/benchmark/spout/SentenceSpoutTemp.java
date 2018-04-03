package com.basic.benchmark.spout;


import com.basic.benchmark.bench.ThroughAvgLatencySpout;
import com.basic.benchmark.util.TimeUtils;
import com.basic.benchmark.util.TopologyUtil;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

/**
 * locate com.basic.benchmark
 * Created by 79875 on 2017/10/17.
 */
public class SentenceSpoutTemp extends ThroughAvgLatencySpout {
    private static Logger logger= LoggerFactory.getLogger(SentenceSpout.class);

    private static final String WORDCOUNT_STREAM_ID="wordcountstream";
    private boolean isSlowDown;
    private long waitTimeMills;

    public SentenceSpoutTemp(long waitTimeMills) {
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
        super.open(map,topologyContext,spoutOutputCollector);
        isSlowDown= TopologyUtil.isSlowDown();
    }

    //向下游输出
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        super.declareOutputFields(outputFieldsDeclarer);
        outputFieldsDeclarer.declareStream(WORDCOUNT_STREAM_ID,new Fields("word"));
    }

    //核心逻辑
    public void nextTuple() {
        super.nextTuple();
        if(isSlowDown){
            TimeUtils.waitForTimeMills(waitTimeMills);
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
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
    }

    @Override
    public void close() {
    }
}

