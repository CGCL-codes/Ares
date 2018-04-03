package com.basic.benchmark;

import com.basic.benchmark.util.TopologyUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.basic.benchmark.Constants.LATENCYTIME_STREAM_ID;

/**
 * locate com.basic.storm.bolt
 * Created by tj on 2017/5/8.
 */
public class WordCounterBolt extends BaseRichBolt {
    private static Logger logger= LoggerFactory.getLogger(WordCounterBolt.class);

    private Map<String, Long> counts = new HashMap<String, Long>();

    private boolean isSlowDown;
    private long waitTimeMills;
    private int taskid;
    private OutputCollector outputCollector;
    public WordCounterBolt(long waitTimeMills) {
        this.waitTimeMills = waitTimeMills;
    }
    private boolean isperpare;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        isSlowDown= TopologyUtil.isSlowDown();
        this.outputCollector=collector;
        isperpare=true;
    }

    @Override
    public void execute(Tuple tuple) {
        if(isperpare){
            logger.info("currentTimeMills:"+System.currentTimeMillis());
            isperpare=false;
        }

        String word = tuple.getStringByField("word");
        Long startTimeMills=tuple.getLongByField("startTimeMills");
        if (!word.isEmpty()) {
            Long count = counts.get(word);
            if (count == null) {
                count = 0L;
            }
            count++;
            counts.put(word, count);
            //outputCollector.emit(WORDCOUNT_STREAM_ID,tuple,new Values(word,count));
        }
        Long endTimeMills=System.currentTimeMillis();
        Long delay=endTimeMills-startTimeMills;
        outputCollector.emit(LATENCYTIME_STREAM_ID,new Values(delay,endTimeMills,taskid));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(LATENCYTIME_STREAM_ID,new Fields("latencytime","timeinfo","taskid"));
    }


    @Override
    public void cleanup() {
    }

}
