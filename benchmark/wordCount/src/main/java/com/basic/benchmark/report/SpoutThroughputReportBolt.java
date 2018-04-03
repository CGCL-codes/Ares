package com.basic.benchmark.report;


import com.basic.benchmark.util.DataBaseUtil;
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
 * 用来统计输出Spout输入源的吞吐量的Bolt
 */
public class SpoutThroughputReportBolt extends BaseRichBolt {
    private static Logger logger= LoggerFactory.getLogger(SpoutThroughputReportBolt.class);

    private boolean isGameSchedule;

    public SpoutThroughputReportBolt(boolean isGameSchedule) {
        this.isGameSchedule = isGameSchedule;
    }

    private OutputCollector outputCollector;
    public static final String ACKCOUNT_STREAM_ID="ackcountstream";
    public static final String LATENCYTIME_STREAM_ID="latencytimestream";

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        logger.info("------------SpoutThroughputReportBolt prepare------------");
    }

    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals(ACKCOUNT_STREAM_ID)) {
            Long currentTimeMills = tuple.getLongByField("timeinfo");
            Long tupplecount = tuple.getLongByField("tuplecount");
            int taskid = tuple.getIntegerByField("taskid");
            //将最后结果插入到数据库中
            Timestamp timestamp = new Timestamp(currentTimeMills);
            if(isGameSchedule)
                DataBaseUtil.insertAresSpoutCount(timestamp, tupplecount, taskid);
            else
                DataBaseUtil.insertDefaultSpoutCount(timestamp, tupplecount, taskid);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
