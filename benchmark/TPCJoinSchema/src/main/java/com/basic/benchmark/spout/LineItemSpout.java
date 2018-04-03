package com.basic.benchmark.spout;

import com.basic.benchmark.JoinDataAnalysis;
import com.basic.benchmark.model.LineItem;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * locate com.basic.benchmark.spout
 * Created by 79875 on 2017/10/20.
 */
public class LineItemSpout extends BaseRichSpout {
    private static Logger logger= LoggerFactory.getLogger(GenderSpout.class);
    private SpoutOutputCollector outputCollector;
    ConcurrentHashMap<UUID,Values> pending; //用来记录tuple的msgID，和tuple
    private BufferedReader bufferedReader= null;
    private int id=0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.outputCollector=collector;
        this.pending=new ConcurrentHashMap<UUID, Values>();
        try {
            bufferedReader = new BufferedReader(new FileReader("/root/TJ/join_data/lineitem.tbl"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        //Storm 的消息ack机制
        LineItem lineItem = JoinDataAnalysis.genderateLineitemData(bufferedReader);
        if(lineItem!=null) {
            Values value = new Values(lineItem.getORDERKEY(), lineItem.getPARTKEY(), lineItem.getSUPPKEY(), lineItem.getLINENUMBER(), lineItem.getQUANTITY(), lineItem.getEXTENDEDPRICEl(), lineItem.getDISCOUNT(), System.currentTimeMillis());
            UUID uuid = UUID.randomUUID();
            pending.put(uuid, value);
            outputCollector.emit(value, uuid);
        }
    }

    @Override
    public void ack(Object msgId) {
        pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        outputCollector.emit(pending.get(msgId),msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ORDERKEY","PARTKEY","SUPPKEY","LINENUMBER","QUANTITY","EXTENDEDPRICEl","DISCOUNT","lineitemtimeinfo"));
    }
}
