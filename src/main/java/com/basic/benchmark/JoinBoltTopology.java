/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basic.benchmark;

import com.basic.benchmark.bolt.JoinBolt;
import com.basic.benchmark.bolt.PrinterBolt;
import com.basic.benchmark.spout.AgeSpout;
import com.basic.benchmark.spout.GenderSpout;
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
 * Created by 79875 on 2017/10/18.
 * 提交stormtopology任务 storm jar aresStorm-1.0-SNAPSHOT.jar com.basic.benchmark.JoinBoltTopology jointopology 3 1 1 1 1
 */
public class JoinBoltTopology {
    private static final String TOPOLOGY_NAME= "join-topology";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Integer numworkers=Integer.valueOf(args[1]);
        Integer genderspoutparallelism=Integer.valueOf(args[2]);
        Integer agespoutparallelism=Integer.valueOf(args[3]);
        Integer joinboltparallelism=Integer.valueOf(args[4]);
        Integer printerboltparallelism=Integer.valueOf(args[5]);

        GenderSpout genderSpout = new GenderSpout();
        AgeSpout ageSpout = new AgeSpout();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(GENDER_SPOUT_ID, genderSpout,genderspoutparallelism);
        builder.setSpout(AGE_SPOUT_ID, ageSpout,agespoutparallelism);

        // inner join of 'age' and 'gender' records on 'id' field
        JoinBolt joiner = new JoinBolt(GENDER_SPOUT_ID, "id")
                .join(AGE_SPOUT_ID,"id", GENDER_SPOUT_ID)
                .select ("gender-spout:id,age-spout:id,gender,age")
                .withTumblingWindow( new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS) );

        builder.setBolt(JOIN_BLOT_ID, joiner,joinboltparallelism)
                .fieldsGrouping(GENDER_SPOUT_ID,GENDER_STREAMID, new Fields("id"))
                .fieldsGrouping(AGE_SPOUT_ID,AGE_STREAMID, new Fields("id"))         ;

        builder.setBolt(PRINT_BOLT_ID, new PrinterBolt(),printerboltparallelism).shuffleGrouping(JOIN_BLOT_ID);

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
