package com.basic.benchmark.report;

import com.basic.benchmark.task.InsertAresSpoutLatencyTask;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * locate com.basic.benchmark
 * Created by 79875 on 2017/10/8.
 */
public class LatencyReportBolt extends BaseRichBolt {
    private static Logger logger = LoggerFactory.getLogger(LatencyReportBolt.class);
    private static final String ACKCOUNT_STREAM_ID = "ackcountstream";
    private static final String LATENCYTIME_STREAM_ID = "latencytimestream";
    private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 10, 200, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        logger.info("------------LatencyReportBolt prepare------------");
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(LATENCYTIME_STREAM_ID)) {
            Long currentTimeMills = tuple.getLongByField("timeinfo");
            Long latencyTime = tuple.getLongByField("latencytime");
            int taskid = tuple.getIntegerByField("taskid");
            //将最后结果插入到数据库中
            Timestamp timestamp = new Timestamp(currentTimeMills);
            InsertAresSpoutLatencyTask task=new InsertAresSpoutLatencyTask(timestamp,latencyTime,taskid);
            executor.execute(task);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
