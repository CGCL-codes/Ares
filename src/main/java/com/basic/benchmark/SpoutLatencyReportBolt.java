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
 * locate com.basic.benchmark
 * Created by 79875 on 2017/10/8.
 */
public class SpoutLatencyReportBolt extends BaseRichBolt {
    private static Logger logger = LoggerFactory.getLogger(SpoutLatencyReportBolt.class);
    public static final String ACKCOUNT_STREAM_ID = "ackcountstream";
    public static final String LATENCYTIME_STREAM_ID = "latencytimestream";

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        logger.info("------------SpoutLatencyReportBolt prepare------------");
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(LATENCYTIME_STREAM_ID)) {
            Long currentTimeMills = tuple.getLongByField("timeinfo");
            Long latencyTime = tuple.getLongByField("latencytime");
            int taskid = tuple.getIntegerByField("taskid");

            //将最后结果插入到数据库中
            Timestamp timestamp = new Timestamp(currentTimeMills);
            DataBaseUtil.insertAresSpoutLatency(timestamp, latencyTime, taskid);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
