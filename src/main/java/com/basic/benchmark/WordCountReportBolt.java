package com.basic.benchmark;


import com.basic.util.DataBaseUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Map;

/**
 * Created by 79875 on 2017/3/7.
 * 用来统计WordCount吞吐量的Bolt
 */
public class WordCountReportBolt extends BaseRichBolt {
    private Logger logger= LoggerFactory.getLogger(WordCountReportBolt.class);
    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        logger.info("------------WordCountReportBolt prepare------------");
    }

    public void execute(Tuple tuple) {
        Long currentTimeMills=tuple.getLongByField("timeinfo");
        Long tupplecount=tuple.getLongByField("tuplecount");
        int taskid=tuple.getIntegerByField("taskid");

        //将最后结果插入到数据库中
        Timestamp timestamp=new Timestamp(currentTimeMills);
        DataBaseUtil.insertAresTupleCount(timestamp,tupplecount,taskid);
        //logger.info("timestamp:"+currentTimeMills+" tupplecount:"+tupplecount);
        //实际应用中，最后一个阶段，大部分应该是持久化到mysql，redis，es，solr或mongodb中
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

