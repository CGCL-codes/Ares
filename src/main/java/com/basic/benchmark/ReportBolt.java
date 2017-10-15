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
 * Created by dello on 2016/10/15.
 * 用来统计输出WordCount结果的Bolt
 */
public class ReportBolt extends BaseRichBolt {

    private Logger logger= LoggerFactory.getLogger(ReportBolt.class);
    private OutputCollector outputCollector;
    private int number;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        number=1;
        logger.info("------------ReportBolt prepare------------");
        //DataBaseUtil.getConnection();
    }

    public void execute(Tuple tuple) {
        String word=tuple.getStringByField("word");
        Long count=tuple.getLongByField("count");

        //将最后结果插入到数据库中
        Timestamp timestamp=new Timestamp(System.currentTimeMillis());
        //logger.info("word:"+word+" tupplecount:"+count +" timeinfo"+timestamp);
        DataBaseUtil.insertAresWordCount(timestamp,word,count,number);
        outputCollector.ack(tuple);
        number++;
        //实际应用中，最后一个阶段，大部分应该是持久化到mysql，redis，es，solr或mongodb中
}

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {

    }
}
