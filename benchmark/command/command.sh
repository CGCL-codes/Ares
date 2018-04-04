#!/bin/bash
# Program:
# 
# Histroy
# 2018/4/4 mastertj Firest release 
PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin
export PATH

#启动nimbus命令
nohup /storm/apache-storm-1.2.1/bin/storm nimbus > /storm/apache-storm-1.2.1/logs/nimbus.out 2>&1
#启动ui命令
nohup /storm/apache-storm-1.2.1/bin/storm ui > /storm/apache-storm-1.2.1/logs/ui.out 2>&1 &

#批量启动storm
/storm/script/start-storm-1.2.1.sh
#批量关闭storm
/storm/script/stop-storm-1.2.1.sh

#进入到zhangfan目录下运行
cd /home/zhangfan

#wordcount 延迟测试命令
storm jar wordCount-1.0-SNAPSHOT.jar com.basic.benchmark.SentenceWordCountLatencyTopology stormwordcount 30 1 30 10 false 60

#wordcount 吞吐量测试命令
storm jar wordCount-1.0-SNAPSHOT.jar com.basic.benchmark.SentenceWordCountThroughputTopology stormwordcount 30 1 30 false 60

#进入数据库
mysql -h node100 -u root -p

#数据库操作命令
use aresbenchmark;
select * from t_aresspoutlatency;
select avg(latencytime) from t_aresspoutlatency;
select * from t_aresspoutlatency order by time desc limit 10;
select avg(latencytime) from (select * from t_aresspoutlatency order by time desc limit 10) as letency;
delete from  t_aresspoutlatency;

throughput
select * from t_aresspoutcount;
select avg(tuplecount) from t_aresspoutcount;
select * from t_aresspoutcount order by time desc limit 10;
select avg(tuplecount) from (select * from t_aresspoutcount order by time desc limit 10) as throughput;
delete from  t_aresspoutcount;
