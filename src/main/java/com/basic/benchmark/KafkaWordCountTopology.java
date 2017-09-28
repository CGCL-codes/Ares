package com.basic.benchmark;

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

/**
 * Created by 79875 on 2017/3/3.
 * storm jar aresStorm-1.0-SNAPSHOT.jar com.basic.benchmark.KafkaWordCountTopology tweetswordtopic3 stormkafka 12 18 10
 */
public class KafkaWordCountTopology {
    public static final String KAFKA_SPOUT_ID ="kafka-spout";
    public static final String COUNT_BOLT_ID = "count-bolt";
    public static final String REPORT_BOLT_IT= "report-bolt";
    public static final String WORDCOUNT_REPORT_BOLT_ID= "wordcount-report-bolt";
    public static final String TOPOLOGY_NAME= "kafka-wordcount-topology";
    public static final String WORDCOUNT_STREAM_ID="wordcountstream";
    public static final String TUPLECOUNT_STREAM_ID="tuplecountstream";
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException {
        String zks = "root2:2181,root4:2181,root5:2181";
//        String topic = "tweetsword2";
        String topic= args[0];
        String zkRoot = "/storm"; // default zookeeper root configuration for storm
        String id = args[1];//设置消费者的ID

        WordCountReportBolt wordCountReportBolt=new WordCountReportBolt();

        Integer numworkers=Integer.valueOf(args[2]);
        Integer spoutparallelism=Integer.valueOf(args[3]);
        Integer countboltparallelism=Integer.valueOf(args[4]);

        BrokerHosts brokerHosts = new ZkHosts(zks,"/kafka/brokers");
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new MyScheme());
        spoutConf.ignoreZkOffsets = true;
        spoutConf.zkServers = Arrays.asList(new String[] {"root2", "root4", "root5"});
        spoutConf.zkPort = 2181;
        //      spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();//从最新消息的开始读取
        spoutConf.startOffsetTime = -2L;//从最旧的消息开始读取

        System.out.println("kafkaspout outputFields num1: "+spoutConf.scheme.getOutputFields().get(0));
        KafkaSpout kafkaSpout=new KafkaSpout(spoutConf);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout, spoutparallelism); // Kafka我们创建了一个6分区的Topic，这里并行度设置为6
        //builder.setBolt(KAFFKA_BOLT_ID, new KaffkaBolt(), kaffkaboltparallelism).shuffleGrouping(SENTENCE_SPOUT_ID);
        builder.setBolt(COUNT_BOLT_ID, new WordCounterBolt(), countboltparallelism)
                .fieldsGrouping(KAFKA_SPOUT_ID,new Fields("word"));
                //.shuffleGrouping(KAFKA_SPOUT_ID);
        builder.setBolt(WORDCOUNT_REPORT_BOLT_ID,wordCountReportBolt)
                .allGrouping(COUNT_BOLT_ID,TUPLECOUNT_STREAM_ID);
        builder.setBolt(REPORT_BOLT_IT, new ReportBolt())
                .shuffleGrouping(COUNT_BOLT_ID,WORDCOUNT_STREAM_ID);

        Config config = new Config();
        config.setNumWorkers(numworkers);
        config.setNumAckers(0);//每个Work进程会运行一个Acker任务，这里将Ack任务设置为0 禁止Ack任务
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
