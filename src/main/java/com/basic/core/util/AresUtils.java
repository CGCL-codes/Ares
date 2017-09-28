package com.basic.core.util;

import com.basic.util.PropertiesUtil;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.resource.Component;

import java.util.*;

/**
 * locate com.basic.util
 * Created by 79875 on 2017/9/25.
 */
public class AresUtils {
    //The parameter W1 and W2 denote the weight of the stream` response time and the stream recovery time, respectively;
    // public static double W1, W2;
    public static double W1 = 0.7, W2 = 0.3;

    public static <K, V> HashMap<V, List<K>> reverseMap(Map<K, V> map) {
        HashMap<V, List<K>> rtn = new HashMap<V, List<K>>();
        if (map == null) {
            return rtn;
        }
        for (Map.Entry<K, V> entry : map.entrySet()) {
            K key = entry.getKey();
            V val = entry.getValue();
            List<K> list = rtn.get(val);
            if (list == null) {
                list = new ArrayList<K>();
                rtn.put(entry.getValue(), list);
            }
            list.add(key);
        }
        return rtn;
    }

    //The parameter q denotes the computation cost for executors to process a single tuple.
    public static Map<ExecutorDetails, Double> initializeQ(List<ExecutorDetails> executors, TopologyDetails topology) {
        PropertiesUtil.init("componentcost.properties");
        Map<ExecutorDetails, Double> q = new HashMap<ExecutorDetails, Double>();
        for(ExecutorDetails executor :executors){
            String currentComponentId = topology.getExecutorToComponent().get(executor);
            q.put(executor, Double.valueOf(PropertiesUtil.getProperties(currentComponentId)));
        }
        return q;
    }

    //The parameter lambda denotes the data processing time for slots to process a single computation cost.
    public static Map<WorkerSlot, Double> initializeLambda(List<WorkerSlot> slots , Cluster cluster) {
        Map<WorkerSlot, Double> lambda = new HashMap<WorkerSlot, Double>();
        //cluster.getHost()
        for (WorkerSlot slot : slots) {
            String nodeId = slot.getNodeId();
        }
        return lambda;
    }

    //The parameter d denotes the data transferring time of node pairs.
    public static  Map<WorkerSlot, Map<WorkerSlot, Double>> initializeD(List<WorkerSlot> slots,Cluster cluster) {
        Map<WorkerSlot, Map<WorkerSlot, Double>> d = new HashMap<WorkerSlot, Map<WorkerSlot, Double>>();
        //   Map<WorkerSlot, Double> temp = new HashMap<WorkerSlot, Double>();
        for (int i = 0; i < slots.size(); i++) {
            for (int j = i; j < slots.size(); j++) {
//                Random random = new Random();
//                double cost = i == j ? 0.0 : random.nextDouble();
//                //   temp.put(slots.get(j),cost);
//                //  d.put(slots.get(i), temp);
//                // WorkerSlot slot = slots.get(j);
//                //   temp.put(slots.get(j),0.0);
//                //  d.put(slots.get(i),temp);
//                //  d.get(slots.get(j)).put(slots.get(i), cost);
//
//                if (d.containsKey(slots.get(i))) {
//                    d.get(slots.get(i)).put(slots.get(j), cost);
//                } else {
//                    Map<WorkerSlot, Double> temp = new HashMap<WorkerSlot, Double>();
//                    temp.put(slots.get(j), cost);
//                    d.put(slots.get(i), temp);
//                }
//                if (d.containsKey(slots.get(j))) {
//                    d.get(slots.get(j)).put(slots.get(i), cost);
//                } else {
//                    Map<WorkerSlot, Double> temp = new HashMap<WorkerSlot, Double>();
//                    temp.put(slots.get(i), cost);
//                    d.put(slots.get(j), temp);
//                }
            }
        }
        return d;
    }

    public static Map<ExecutorDetails, Map<ExecutorDetails, Double>>  initializeW(List<ExecutorDetails> executors) {
        Map<ExecutorDetails, Map<ExecutorDetails, Double>> w = new HashMap<ExecutorDetails, Map<ExecutorDetails, Double>>();
        w.clear();
        for (int i = 0; i < executors.size(); i++) {
            for (int j = i; j < executors.size(); j++) {
                Random random = new Random();
                double cost = i == j ? 0.0 : random.nextDouble();
                if (w.containsKey(executors.get(i))) {
                    w.get(executors.get(i)).put(executors.get(j), cost);
                } else {
                    Map<ExecutorDetails, Double> temp = new HashMap<ExecutorDetails, Double>();
                    temp.put(executors.get(j), cost);
                    w.put(executors.get(i), temp);
                }
                if (w.containsKey(executors.get(j))) {
                    w.get(executors.get(j)).put(executors.get(i), cost);
                } else {
                    Map<ExecutorDetails, Double> temp = new HashMap<ExecutorDetails, Double>();
                    temp.put(executors.get(i), cost);
                    w.put(executors.get(j), temp);
                }
            }
        }
        return w;
    }

    /**
     * 递归调用得到当前孩子路径数量，然后计算总路径数量
     * @param topology
     * @param component
     * @return
     */
    public static int getChildsPathsNumber(TopologyDetails topology,Component component){
        int temp=0;
        List<String> childrensId = component.children;
        if(childrensId.size()==0)
            return component.execs.size();
        for(String childId :childrensId){
            Component child = topology.getComponents().get(childId);
            temp+=component.execs.size()*getChildsPathsNumber(topology,child);
        }
        return temp;
    }

    /**
     * 递归调用得到当前父母路径数量，然后计算总路径数量
     * @param topology
     * @param component
     * @return
     */
    public static int getParentsPathsNumber(TopologyDetails topology,Component component){
        int temp=0;
        List<String> parentsId = component.parents;
        if(parentsId.size()==0)
            return component.execs.size();
        for(String parentId :parentsId){
            Component parent = topology.getComponents().get(parentId);
            temp+=component.execs.size()*getChildsPathsNumber(topology,parent);
        }
        return temp;
    }


    public static  Map<ExecutorDetails, Double> initializeAlpha(TopologyDetails topology, List<ExecutorDetails> executors) {
        Map<ExecutorDetails, Double> alpha = new HashMap<>();

        //Calculate the total number of paths
        int allPathLength=0;

        Map<String, SpoutSpec> spouts = topology.getTopology().get_spouts();
        for(String spoutId:spouts.keySet()){
            Component spout = topology.getComponents().get(spoutId);
            allPathLength+=getChildsPathsNumber(topology,spout);
        }

        for(ExecutorDetails executor:executors){
            String componentId = topology.getExecutorToComponent().get(executor);
            Component component = topology.getComponents().get(componentId);
            if(component==null){
                break;
            }
            int childPathNumber=getChildsPathsNumber(topology,component);
            int parentPathNumber=getParentsPathsNumber(topology,component);
            int currentExecutorPathNumber=parentPathNumber*childPathNumber;
            PropertiesUtil.init("componentcost.properties");
            alpha.put(executor, Double.valueOf(currentExecutorPathNumber/allPathLength)*Double.valueOf(PropertiesUtil.getProperties(componentId)));
        }
        return alpha;
    }

    public static Map<ExecutorDetails, Map<ExecutorDetails, Double>> initializeBeta(TopologyDetails topology, List<ExecutorDetails> executors) {
        Map<ExecutorDetails, Map<ExecutorDetails, Double>> beta = new HashMap<>();
        //Calculate the total number of paths
        int allPathLength=0;

        Map<String, SpoutSpec> spouts = topology.getTopology().get_spouts();
        for(String spoutId:spouts.keySet()){
            Component spout = topology.getComponents().get(spoutId);
            allPathLength+=getChildsPathsNumber(topology,spout);
        }

        for(ExecutorDetails executor:executors) {
            String componentId = topology.getExecutorToComponent().get(executor);
            Component component = topology.getComponents().get(componentId);
            if (component == null || component.children.size()==0) {
                break;
            }
            Iterator<String> childcomponentIdIterator = component.children.iterator();
            while (childcomponentIdIterator.hasNext()){
                String childcomponentId = childcomponentIdIterator.next();
                Component childcomponent = topology.getComponents().get(childcomponentId);
                for(ExecutorDetails childexecutor:childcomponent.execs){
                    int parentPathNumber=getParentsPathsNumber(topology,component);
                    int childPathNumber=getChildsPathsNumber(topology,childcomponent);
                    int currentPathNumber=parentPathNumber*childPathNumber;
                    Map<ExecutorDetails, Double> temp=new HashMap<>();
                    if(beta.containsKey(executor))
                        temp=beta.get(executor);
                    temp.put(childexecutor,Double.valueOf(currentPathNumber/allPathLength));
                    beta.put(executor, temp);
                }
            }
        }
        return beta;
    }

    public static Map<ExecutorDetails, Map<ExecutorDetails, Double>>  initializeGamma(Cluster cluster, List<ExecutorDetails> executors) {
        Map<ExecutorDetails, Map<ExecutorDetails, Double>> gamma = new HashMap<>();

        return gamma;
    }
}
