package com.basic.benchmark;

import com.basic.benchmark.bolt.report.SpouThroughputReportBolt;
import com.basic.benchmark.bolt.WordCounterBolt;
import com.basic.benchmark.spout.WordCountKafkaSpout;
import com.basic.util.MyScheme;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;

import static com.basic.benchmark.Constants.*;

/**
 * Created by 79875 on 2017/3/3.
 * storm jar aresStorm-1.0-SNAPSHOT.jar com.basic.benchmark.KafkaWordCountTopology tweetswordtopic3 stormkafka 9 9 9 true
 */
public class KafkaWordCountTopology {
    private static final String TOPOLOGY_NAME= "sentence-wordcount-topology";
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException {
        String zks = "root2:2181,root4:2181,root5:2181";
//        String topic = "tweetsword2";
        String topic= args[0];
        String zkRoot = "/stormkafka"; // default zookeeper root configuration for storm
        String id = args[1];//设置消费者的ID

        Integer numworkers=Integer.valueOf(args[2]);
        Integer spoutparallelism=Integer.valueOf(args[3]);
        Integer countboltparallelism=Integer.valueOf(args[4]);
        Long waitTimeMills=Long.valueOf(args[5]);

        BrokerHosts brokerHosts = new ZkHosts(zks,"/kafka/brokers");
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new MyScheme());
        spoutConf.ignoreZkOffsets = true;
        spoutConf.zkServers = Arrays.asList(new String[] {"root2", "root4", "root5"});
        spoutConf.zkPort = 2181;
        //      spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();//从最新消息的开始读取
        spoutConf.startOffsetTime = -2L;//从最旧的消息开始读取

        System.out.println("kafkaspout outputFields num1: "+spoutConf.scheme.getOutputFields().get(0));
        WordCountKafkaSpout kafkaSpout=new WordCountKafkaSpout(spoutConf);

        Boolean isGameSchedule=Boolean.valueOf(args[5]);
        SpouThroughputReportBolt spouThroughputReportBolt=new SpouThroughputReportBolt(isGameSchedule);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout, spoutparallelism);
        builder.setBolt(COUNT_BOLT_ID, new WordCounterBolt(waitTimeMills), countboltparallelism)
                .fieldsGrouping(KAFKA_SPOUT_ID,WORDCOUNT_STREAM_ID,new Fields("word"));
                //.shuffleGrouping(KAFKA_SPOUT_ID);
        builder.setBolt(SPOUT_THROUGHPUTREPORT_BOLT_ID,spouThroughputReportBolt)
                .allGrouping(KAFKA_SPOUT_ID,ACKCOUNT_STREAM_ID);

        Config config = new Config();
        config.setNumWorkers(numworkers);

        if (args != null && args.length > 0) {
            // Nimbus host name passed from command line
            StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, config, builder.createTopology());
        } else {
            config.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
            Thread.sleep(60000);
            cluster.shutdown();
        }
    }
}
