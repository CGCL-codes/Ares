package com.basic.benchmark.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.basic.benchmark.Constants.AGE_STREAMID;

/**
 * locate com.basic.benchmark.spout
 * Created by 79875 on 2017/10/18.
 */
public class AgeSpout extends BaseRichSpout {
    private static Logger logger= LoggerFactory.getLogger(AgeSpout.class);
    private SpoutOutputCollector outputCollector;
    ConcurrentHashMap<UUID,Values> pending; //用来记录tuple的msgID，和tuple

    private long id=0L;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.outputCollector=collector;
        this.pending=new ConcurrentHashMap<UUID, Values>();
    }

    @Override
    public void nextTuple() {
        id++;
        Random random=new Random();
        //Storm 的消息ack机制
        Values value = new Values(id,random.nextInt(20)+1);
        UUID uuid=UUID.randomUUID();
        pending.put(uuid,value);

        outputCollector.emit(AGE_STREAMID,value,uuid);
    }

    @Override
    public void ack(Object msgId) {
        pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        outputCollector.emit(AGE_STREAMID,pending.get(msgId),msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(AGE_STREAMID,new Fields("id","age"));
    }
}
