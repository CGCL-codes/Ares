//package com.basic.benchmark.spout;
//
//import org.apache.storm.spout.SpoutOutputCollector;
//import org.apache.storm.task.TopologyContext;
//import org.apache.storm.topology.OutputFieldsDeclarer;
//import org.apache.storm.topology.base.BaseRichSpout;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Map;
//import java.util.UUID;
//
///**
// * locate com.basic.benchmark.spout
// * Created by 79875 on 2017/10/18.
// */
//public class GenderSpout extends BaseRichSpout {
//    private static Logger logger= LoggerFactory.getLogger(GenderSpout.class);
//    private SpoutOutputCollector spoutOutputCollector;
//
//    private String randomWords(int wordLength){
//        char[] chars=new char[wordLength];
//        for(int i=0;i<wordLength;i++){
//            char c=(char)('A'+Math.random()*('Z'-'A'+1));
//            chars[i]=c;
//        }
//        return new String(chars);
//    }
//
//
//    @Override
//    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
//        this.spoutOutputCollector=spoutOutputCollector;
//    }
//
//    @Override
//    public void nextTuple() {
//        String word=randomWords(5);
//        //Storm 的消息ack机制
//        Values value = new Values(word);
//        UUID uuid=UUID.randomUUID();
//        pending.put(uuid,value);
//        latencyHashMap.put(uuid,System.currentTimeMillis());
//
//        outputCollector.emit(WORDCOUNT_STREAM_ID,value,uuid);
//    }
//
//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//
//    }
//}
