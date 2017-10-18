package com.basic.benchmark;

import com.basic.benchmark.bolt.report.LatencyReportBolt;
import com.basic.benchmark.bolt.report.SpouThroughputReportBolt;
import com.basic.benchmark.bolt.WordCounterBolt;
import com.basic.benchmark.spout.SentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import static com.basic.benchmark.Constants.*;

/**
 * locate com.basic.benchmark.schedule
 * Created by 79875 on 2017/10/17.
 * 提交stormtopology任务 storm jar aresStorm-1.0-SNAPSHOT.jar com.basic.benchmark.ResourceWordCountTopology stormwordcount 9 9 9 false 60
 */
public class ResourceWordCountTopology {

    private static final String TOPOLOGY_NAME= "sentence-wordcount-topology";
    public static void main(String[] args) throws Exception {
//        LatencyReportBolt latencyReportBolt=new LatencyReportBolt();
        //WordCountReportBolt wordCountReportBolt=new WordCountReportBolt();

        TopologyBuilder builder=new TopologyBuilder();
        Integer numworkers=Integer.valueOf(args[1]);
        Integer spoutparallelism=Integer.valueOf(args[2]);
        Integer wordcountboltparallelism=Integer.valueOf(args[3]);

        Boolean isGameSchedule=Boolean.valueOf(args[4]);
        Long waitTimeMills=Long.valueOf(args[5]);
        SentenceSpout spout=new SentenceSpout(waitTimeMills);
        WordCounterBolt wordCountBolt=new WordCounterBolt(waitTimeMills);
        LatencyReportBolt latencyReportBolt =new LatencyReportBolt();
        SpouThroughputReportBolt spouThroughputReportBolt=new SpouThroughputReportBolt(isGameSchedule);

        SpoutDeclarer spoutDeclarer = builder.setSpout(SENTENCE_SPOUT_ID, spout, spoutparallelism);
        BoltDeclarer wordCDeclarer = builder.setBolt(COUNT_BOLT_ID, wordCountBolt, wordcountboltparallelism)
                .fieldsGrouping(SENTENCE_SPOUT_ID, WORDCOUNT_STREAM_ID, new Fields("word"));
        BoltDeclarer throughputBoltDeclarer = builder.setBolt(SPOUT_THROUGHPUTREPORT_BOLT_ID, spouThroughputReportBolt)
                .allGrouping(SENTENCE_SPOUT_ID, ACKCOUNT_STREAM_ID);
        BoltDeclarer latencyBoltDeclarer1 = builder.setBolt(SPOUT_LATENCYREPORT_BOLT_ID, latencyReportBolt)
                .allGrouping(SENTENCE_SPOUT_ID, LATENCYTIME_STREAM_ID);

        spoutDeclarer.setCPULoad(20);
        wordCDeclarer.setCPULoad(40);
        throughputBoltDeclarer.setCPULoad(50);
        //latencyBoltDeclarer1.setCPULoad(50);

        //Topology配置
        Config config=new Config();
        config.setNumWorkers(numworkers);//设置两个Worker进程 10

        config.setTopologyWorkerMaxHeapSize(4096.0);

        //topology priority describing the importance of the topology in decreasing importance starting from 0 (i.e. 0 is the highest priority and the priority importance decreases as the priority number increases).
        //Recommended range of 0-29 but no hard limit set.
        config.setTopologyPriority(29);

        // Set strategy to schedule topology. If not specified, default to org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy
        config.setTopologyStrategy(org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class);

        if(args[0].equals("local")){
            LocalCluster localCluster=new LocalCluster();

            localCluster.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
            Utils.sleep(50*1000);//50s
            localCluster.killTopology(TOPOLOGY_NAME);
            localCluster.shutdown();
        }else {
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }

    }
}
