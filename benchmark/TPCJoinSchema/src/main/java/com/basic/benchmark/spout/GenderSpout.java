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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.basic.benchmark.Constants.GENDER_STREAMID;

/**
 * locate com.basic.benchmark.spout
 * Created by 79875 on 2017/10/18.
 */
public class GenderSpout extends BaseRichSpout {
    private static Logger logger= LoggerFactory.getLogger(GenderSpout.class);
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
        String gender="";
        if(Math.random()<0.5)
            gender="famale";
        else gender="male";

        //Storm 的消息ack机制
        Values value = new Values(id,gender,System.currentTimeMillis());
        UUID uuid=UUID.randomUUID();
        pending.put(uuid,value);

        outputCollector.emit(GENDER_STREAMID,value,uuid);
    }

    @Override
    public void ack(Object msgId) {
        pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        outputCollector.emit(GENDER_STREAMID,pending.get(msgId),msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(GENDER_STREAMID,new Fields("id","gender","gendertimeinfo"));
    }
}
