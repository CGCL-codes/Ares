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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by 79875 on 2017/3/7.
 * 用来统计输出Spout输入源的吞吐量的Bolt
 */
public class SpoutReportBolt extends BaseRichBolt {
    private static Logger logger= LoggerFactory.getLogger(SpoutReportBolt.class);
    private OutputCollector outputCollector;
    private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 10, 200, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        logger.info("------------SpoutReportBolt prepare------------");
    }

    public void execute(Tuple tuple) {
        Long currentTimeMills=tuple.getLongByField("timeinfo");
        Long tupplecount=tuple.getLongByField("tuplecount");
        int taskid=tuple.getIntegerByField("taskid");

        //将最后结果插入到数据库中
        Timestamp timestamp=new Timestamp(currentTimeMills);
        DataBaseUtil.insertAresSpoutCount(timestamp,tupplecount,taskid);
        //实际应用中，最后一个阶段，大部分应该是持久化到mysql，redis，es，solr或mongodb中
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
