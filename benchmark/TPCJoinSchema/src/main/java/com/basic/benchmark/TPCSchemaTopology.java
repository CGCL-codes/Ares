package com.basic.benchmark;

import com.basic.benchmark.bolt.JoinBolt;
import com.basic.benchmark.bolt.TPCSchemePrinterBolt;
import com.basic.benchmark.spout.LineItemSpout;
import com.basic.benchmark.spout.OrdersSPout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.concurrent.TimeUnit;

import static com.basic.benchmark.Constants.*;

/**
 * locate com.basic.benchmark
 * Created by 79875 on 2017/10/20.
 * 提交stormtopology任务 storm jar aresStorm-1.0-SNAPSHOT.jar com.basic.benchmark.TPCSchemaTopology tpcSchematopology 3 1 1 9 1
 */
public class TPCSchemaTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Integer numworkers=Integer.valueOf(args[1]);
        Integer genderspoutparallelism=Integer.valueOf(args[2]);
        Integer agespoutparallelism=Integer.valueOf(args[3]);
        Integer joinboltparallelism=Integer.valueOf(args[4]);
        Integer printerboltparallelism=Integer.valueOf(args[5]);

        LineItemSpout lineItemSpout=new LineItemSpout();
        OrdersSPout ordersSPout=new OrdersSPout();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(LINTEITEM_SPOUT_ID, lineItemSpout,genderspoutparallelism);
        builder.setSpout(ORDERS_SPOUT_ID, ordersSPout,agespoutparallelism);

        // inner join of 'age' and 'gender' records on 'id' field
        JoinBolt joiner = new JoinBolt(LINTEITEM_SPOUT_ID, "ORDERKEY")
                .leftJoin(ORDERS_SPOUT_ID,"ORDERKEY", LINTEITEM_SPOUT_ID)
                .select ("lineitem:ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,CUSTKEY,ORDERSTATUS,TOTALPRICE,CLERK,lineitemtimeinfo,orderstimeinfo")
                .withTumblingWindow( new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS) );

//        SingleJoinBolt joiner=new SingleJoinBolt(new Fields("PARTKEY","SUPPKEY","LINENUMBER","CUSTKEY","ORDERSTATUS","TOTALPRICE","CLERK","lineitemtimeinfo","orderstimeinfo"));
        builder.setBolt(JOIN_BLOT_ID, joiner,joinboltparallelism)
                .fieldsGrouping(LINTEITEM_SPOUT_ID, new Fields("ORDERKEY"))
                .fieldsGrouping(ORDERS_SPOUT_ID, new Fields("ORDERKEY"));

        builder.setBolt(PRINT_BOLT_ID, new TPCSchemePrinterBolt(),printerboltparallelism).shuffleGrouping(JOIN_BLOT_ID);
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
            StormSubmitter.submitTopologyWithProgressBar(args[0],config,builder.createTopology());
        }
    }
}
