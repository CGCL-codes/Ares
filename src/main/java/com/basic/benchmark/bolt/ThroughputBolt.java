package com.basic.benchmark.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import static com.basic.benchmark.Constants.TUPLECOUNT_STREAM_ID;

/**
 * locate com.basic.benchmark.bolt
 * Created by 79875 on 2017/10/18.
 */
public class ThroughputBolt extends BaseRichBolt {
    protected OutputCollector outputCollector;
    protected long tupplecount=0; //记录单位时间通过的元组数量
    protected int thisTaskId =0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        thisTaskId = context.getThisTaskIndex();
        this.outputCollector = collector;
        Timer wordcountTimer = new Timer();
        //设置计时器没1s计算时间
        wordcountTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                //executor.execute(new WordCountTupleTask(new Timestamp(System.currentTimeMillis()),tupplecount));
                outputCollector.emit(TUPLECOUNT_STREAM_ID,new Values(tupplecount,System.currentTimeMillis(),thisTaskId));
                tupplecount = 0;
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒
    }

    @Override
    public void execute(Tuple input) {
        tupplecount++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TUPLECOUNT_STREAM_ID,new Fields("tuplecount","timeinfo","taskid"));
    }
}
