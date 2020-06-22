# AresStorm
AresStorm is a high performance and fault tolerant DSPS. Ares considers both system performance and fault tolerant capability during task allocation. In the design of Ares, we formalize the problem of Fault Tolerant Scheduler (FTS) for finding an optimal task allocation which maximizes the system utility. We use a game-theoretic approach to solve the FTS problem and propose a novel Nirvana algorithm based on best-response dynamics. We mathematically prove the existence of Nash equilibrium in the FTS game. AresStorm greatly outperforms Storm on skew distributed data in terms of system throughput and the average processing latency.

## Introduction
xxx


## How to use?

### Environment

We implement PStream atop Apache Storm (version 1.2.1 or higher), and deploy the system on a cluster. Each machine is equipped with an octa-core 2.4GHz Xeon CPU, 64.0GB RAM, and a 1000Mbps Ethernet interface card. One machine in the cluster serves as the master node to host the Storm Nimbus. The other machines run Storm supervisors.

### Initial Setting

Install Apache Storm (Please refer to http://storm.apache.org/ to learn more).

Install Apace Maven (Please refer to http://maven.apache.org/ to learn more).

Build and package the example

```txt
mvn clean package -Dmaven.test.skip=true
```

### Configurations

Configuration including the following

```txt
./ares-core/src/main/resources/componentcost.properties. (by default)
./ares-core/src/main/resources/componentToNodecost.properties. (by default)
./ares-core/src/main/resources/nodecomputecost.properties. (by default)
./ares-core/src/main/resources/nodetransferpair.properties. (by default)
```

### Using AresStorm

If you already deploy the Apache Storm cluster environment, you only need to replace these jars to `$STORM_HOME/lib` and `$STORM_HOME/lib-worker`
> * ares-core-1.0-SNAPSHOT.jar

#### ares-core.jar
ares-core module source code is maintained using [Maven](http://maven.apache.org/). Generate the excutable jar by running
```
cd ares-core
mvan clean install -Dmaven.test.skip=true -Dcheckstyle.skip=true
```

In the `storm.yaml` configuration of the nimbus node, perform the following configuration:

``` yaml
storm.scheduler: "com.basic.core.scheduler.GameScheduler"
```

### AresStorm Benchmark

#### Building Benchmark
AresStorm benchmark code is maintained using [Maven](http://maven.apache.org/). Generate the excutable jar by running
```
cd benchmark/xxx
mvan clean install -Dmaven.test.skip=true -Dcheckstyle.skip=true
```

#### Running Benchmark

After deploying a AresStorm cluster, you can launch AresStorm by submitting its jar to the cluster. Please refer to Storm documents for how to
[set up a Storm cluster](https://storm.apache.org/documentation/Setting-up-a-Storm-cluster.html) and [run topologies on a Storm cluster](https://storm.apache.org/documentation/Running-topologies-on-a-production-cluster.ht)

Then, you can submit the example to the AresStorm cluster

```txt
storm jar wordCount-1.0-SNAPSHOT.jar com.basic.benchmark.SentenceWordCountThroughputTopology StormWordcountTopollgy *PARALLISM*
```

## Publications

If you want to know more detailed information, please refer to this paper:

Changfu Lin, Jingjing Zhan, Hanhua Chen, Jie Tan, Hai Jin.  "[Ares: A High Performance and Fault-Tolerant Distributed Stream Processing System](https://ieeexplore.ieee.org/document/8526815/)" in Proceedings of 26th International Conference on Network Protocols (ICNP 2018), TCambridge, UK, September 25-27, 2018 ([Bibtex](AresStorm-conf.bib))

## Author and Copyright

AresStorm is developed in Cluster and Grid Computing Lab, Services Computing Technology and System Lab, Big Data Technology and System Lab, School of Computer Science and Technology, Huazhong University of Science and Technology, Wuhan, China by Changfu Lin (lcf@hust.edu.cn), Jingjing Zhan (zjj@hust.edu.cn), Hanhua Chen (chen@hust.edu.cn), Jie Tan(tjmaster@hust.edu.cn), Hai Jin (hjin@hust.edu.cn)

Copyright (C) 2017, [STCS & CGCL](http://grid.hust.edu.cn/) and [Huazhong University of Science and Technology](http://www.hust.edu.cn).


