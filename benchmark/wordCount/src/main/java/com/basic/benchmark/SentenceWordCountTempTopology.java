package com.basic.benchmark;

import com.basic.benchmark.report.LatencyReportBolt;
import com.basic.benchmark.report.SpouThroughputReportBolt;
import com.basic.benchmark.spout.SentenceSpoutTemp;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import static com.basic.benchmark.Constants.*;

/**
 * Created by 79875 on 2017/3/7.
 * 提交stormtopology任务 storm jar wordCount-1.0-SNAPSHOT.jar com.basic.benchmark.SentenceWordCountTempTopology stormwordcount 9 9 9 1 false 60
 */
public class SentenceWordCountTempTopology {

    private static final String TOPOLOGY_NAME= "sentence-wordcount-topology";
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder=new TopologyBuilder();
        Integer numworkers=Integer.valueOf(args[1]);
        Integer spoutparallelism=Integer.valueOf(args[2]);
        Integer wordcountboltparallelism=Integer.valueOf(args[3]);
        Integer latencyreportboltparallelism=Integer.valueOf(args[4]);

        Boolean isGameSchedule=Boolean.valueOf(args[5]);
        Long waitTimeMills=Long.valueOf(args[6]);
        SentenceSpoutTemp spout=new SentenceSpoutTemp(waitTimeMills);
        WordCounterBolt wordCountBolt=new WordCounterBolt(waitTimeMills);
        LatencyReportBolt latencyReportBolt =new LatencyReportBolt();
        SpouThroughputReportBolt spouThroughputReportBolt=new SpouThroughputReportBolt(isGameSchedule);

        builder.setSpout(SENTENCE_SPOUT_ID,spout,spoutparallelism);
        builder.setBolt(COUNT_BOLT_ID,wordCountBolt,wordcountboltparallelism)
                .fieldsGrouping(SENTENCE_SPOUT_ID,WORDCOUNT_STREAM_ID,new Fields("word"));
        builder.setBolt(SPOUT_THROUGHPUTREPORT_BOLT_ID,spouThroughputReportBolt)
                .allGrouping(SENTENCE_SPOUT_ID,ACKCOUNT_STREAM_ID);
        builder.setBolt(SPOUT_LATENCYREPORT_BOLT_ID, latencyReportBolt,latencyreportboltparallelism)
                .shuffleGrouping(SENTENCE_SPOUT_ID,LATENCYTIME_STREAM_ID);

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

