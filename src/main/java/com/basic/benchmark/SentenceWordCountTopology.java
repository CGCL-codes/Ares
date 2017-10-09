package com.basic.benchmark;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * Created by 79875 on 2017/3/7.
 * 提交stormtopology任务 storm jar aresStorm-1.0-SNAPSHOT.jar com.basic.benchmark.SentenceWordCountTopology stormwordcount 9 9 9 true
 */
public class SentenceWordCountTopology {
    public static final String SENTENCE_SPOUT_ID ="sentence-spout";
    public static final String COUNT_BOLT_ID = "count-bolt";
    public static final String REPORT_BOLT_ID= "report-bolt";
    public static final String SPOUT_THROUGHPUTREPORT_BOLT_ID= "spout-throughputreport-bolt";
    public static final String SPOUT_LATENCYREPORT_BOLT_ID= "spout-latencyreport-bolt";
    public static final String TOPOLOGY_NAME= "sentence-wordcount-topology";
    public static final String WORDCOUNT_STREAM_ID="wordcountstream";
    public static final String ACKCOUNT_STREAM_ID="ackcountstream";
    public static final String LATENCYTIME_STREAM_ID="latencytimestream";

    public static void main(String[] args) throws Exception {

        SentenceSpout spout=new SentenceSpout();
        WordCounterBolt wordCountBolt=new WordCounterBolt();

//        SpoutLatencyReportBolt spoutLatencyReportBolt=new SpoutLatencyReportBolt();
        //WordCountReportBolt wordCountReportBolt=new WordCountReportBolt();

        TopologyBuilder builder=new TopologyBuilder();
        Integer numworkers=Integer.valueOf(args[1]);
        Integer spoutparallelism=Integer.valueOf(args[2]);
        Integer wordcountboltparallelism=Integer.valueOf(args[3]);

        Boolean isGameSchedule=Boolean.valueOf(args[4]);
        SpouThroughputReportBolt spouThroughputReportBolt=new SpouThroughputReportBolt(isGameSchedule);

        builder.setSpout(SENTENCE_SPOUT_ID,spout,spoutparallelism);
        builder.setBolt(COUNT_BOLT_ID,wordCountBolt,wordcountboltparallelism)
                .fieldsGrouping(SENTENCE_SPOUT_ID,WORDCOUNT_STREAM_ID,new Fields("word"));
        builder.setBolt(SPOUT_THROUGHPUTREPORT_BOLT_ID,spouThroughputReportBolt)
                .allGrouping(SENTENCE_SPOUT_ID,ACKCOUNT_STREAM_ID);
//        builder.setBolt(SPOUT_LATENCYREPORT_BOLT_ID,spoutLatencyReportBolt)
//                .allGrouping(SENTENCE_SPOUT_ID,LATENCYTIME_STREAM_ID);

        //Topology配置
        Config config=new Config();
        config.setNumWorkers(numworkers);//设置两个Worker进程 10
        //config.setNumAckers(0);//每个Work进程会运行一个Acker任务，这里将Ack任务设置为0 禁止Ack任务
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

