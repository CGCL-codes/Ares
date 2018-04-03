package com.basic.benchmark.spout;

import com.basic.benchmark.bench.ThroughputSpout;
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

import static com.basic.benchmark.Constants.WORDCOUNT_STREAM_ID;

/**
 * Created by dello on 2016/10/15.
 */
public class SentenceThroughputSpout extends ThroughputSpout {

    private static Logger logger= LoggerFactory.getLogger(SentenceThroughputSpout.class);

    private boolean isSlowDown;
    private long waitTimeMills;

    public SentenceThroughputSpout(long waitTimeMills) {
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
        logger.info("------------SentenceThroughputSpout open------------");
        super.open(map,topologyContext,spoutOutputCollector);
        isSlowDown= TopologyUtil.isSlowDown();
    }

    //向下游输出
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        super.declareOutputFields(outputFieldsDeclarer);
        outputFieldsDeclarer.declareStream(WORDCOUNT_STREAM_ID,new Fields("word","startTimeMills"));
    }

    //核心逻辑
    public void nextTuple() {
        if(isSlowDown){
            TimeUtils.waitForTimeMills(waitTimeMills);
        }

        String word=randomWords(5);
        //Storm 的消息ack机制
        Values value = new Values(word,System.currentTimeMillis());
        UUID uuid=UUID.randomUUID();
        pending.put(uuid,value);
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
