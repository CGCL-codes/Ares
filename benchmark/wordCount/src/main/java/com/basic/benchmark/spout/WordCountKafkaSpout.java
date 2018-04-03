package com.basic.benchmark.spout;

import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * locate com.basic.benchmark
 * Created by 79875 on 2017/10/12.
 */
public class WordCountKafkaSpout extends KafkaSpout {
    private ConcurrentHashMap<UUID,Long> latencyHashMap; //用来统计tuple的延迟信息的HashMap
    private Timer timer;
    private int thisTaskId =0;
    private long ackcount=0; //记录单位时间ACK的元组数量
    private SpoutOutputCollector spoutOutputCollector;
    private static final String ACKCOUNT_STREAM_ID="ackcountstream";
    private static final String WORDCOUNT_STREAM_ID="wordcountstream";

    public WordCountKafkaSpout(SpoutConfig spoutConf) {
        super(spoutConf);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);

        this.spoutOutputCollector=collector;
        thisTaskId=context.getThisTaskId();
        timer=new Timer();
        //设置计时器没1s计算时间
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                //executor.execute(new WordCountTupleTask(new Timestamp(System.currentTimeMillis()),spoutcount));
                spoutOutputCollector.emit(ACKCOUNT_STREAM_ID,new Values(ackcount,System.currentTimeMillis(),thisTaskId));
                ackcount = 0;
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);

        ackcount++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(WORDCOUNT_STREAM_ID,new Fields("word"));
        outputFieldsDeclarer.declareStream(ACKCOUNT_STREAM_ID,new Fields("tuplecount","timeinfo","taskid"));
    }
}
