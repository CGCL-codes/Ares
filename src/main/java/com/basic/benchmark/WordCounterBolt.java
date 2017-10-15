package com.basic.benchmark;

import com.basic.core.util.AresUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * locate com.basic.storm.bolt
 * Created by tj on 2017/5/8.
 */
public class WordCounterBolt extends BaseRichBolt {
    private static Logger logger= LoggerFactory.getLogger(WordCounterBolt.class);

    public static final String WORDCOUNT_STREAM_ID="wordcountstream";
    public static final String TUPLECOUNT_STREAM_ID="tuplecountstream";

    private Map<String, Long> counts = new HashMap<String, Long>();
    private OutputCollector outputCollector;
    private List<Tuple> tupleList=new ArrayList<>();

    private Timer timer;
    private Timer wordcountTimer;
    private long tupplecount=0; //记录单位时间通过的元组数量
    private int thisTaskId =0;

    private boolean isSlowDown;
    private long waitTimeMills;

    public WordCounterBolt(long waitTimeMills) {
        this.waitTimeMills = waitTimeMills;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        thisTaskId = context.getThisTaskIndex();
        this.outputCollector = collector;
        timer=new Timer();
        wordcountTimer=new Timer();
        isSlowDown=AresUtils.isSlowDown();

        //设置计时器没1s计算时间
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                //executor.execute(new WordCountTupleTask(new Timestamp(System.currentTimeMillis()),tupplecount));
                outputCollector.emit(TUPLECOUNT_STREAM_ID,new Values(tupplecount,System.currentTimeMillis(),thisTaskId));
                tupplecount = 0;
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒

//        wordcountTimer.scheduleAtFixedRate(new TimerTask() {
//            public void run() {
//                if(tupleList.size()==0){
//
//                }
//            }
//        }, 1,500);// 设定指定的时间time,此处为1000毫秒
    }

    @Override
    public void execute(Tuple tuple) {
        tupplecount++;
        String word = tuple.getStringByField("word");
        if (!word.isEmpty()) {
            Long count = counts.get(word);
            if (count == null) {
                count = 0L;
            }
            count++;
            counts.put(word, count);
            //outputCollector.emit(WORDCOUNT_STREAM_ID,tuple,new Values(word,count));
        }
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declareStream(WORDCOUNT_STREAM_ID,new Fields("word", "count"));
        declarer.declareStream(TUPLECOUNT_STREAM_ID,new Fields("tuplecount","timeinfo","taskid"));
    }


    @Override
    public void cleanup() {
    }

}
