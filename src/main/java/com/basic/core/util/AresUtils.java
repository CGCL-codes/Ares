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
 * Ares 工具包
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
        PropertiesUtil.init("nodecomputecost.properties");
        Map<WorkerSlot, Double> lambda = new HashMap<WorkerSlot, Double>();
        //cluster.getHost()
        for (WorkerSlot slot : slots) {
            lambda.put(slot,Double.valueOf(PropertiesUtil.getProperties(cluster.getHost(slot.getNodeId()))));
        }
        return lambda;
    }

    //The parameter d denotes the data transferring time of node pairs.
    public static  Map<WorkerSlot, Map<WorkerSlot, Double>> initializeD(List<WorkerSlot> slots,Cluster cluster) {
        PropertiesUtil.init("nodetransferpair.properties");
        Map<WorkerSlot, Map<WorkerSlot, Double>> d = new HashMap<WorkerSlot, Map<WorkerSlot, Double>>();
        //   Map<WorkerSlot, Double> temp = new HashMap<WorkerSlot, Double>();
        for (int i = 0; i < slots.size(); i++) {
            for (int j = 0; j < slots.size(); j++) {
                WorkerSlot slot1 = slots.get(i);
                WorkerSlot slot2 = slots.get(j);
                Double transferTime= Double.valueOf(PropertiesUtil.getProperties(cluster.getHost(slot1.getNodeId())+","+cluster.getHost(slot2.getNodeId())));
                Map<WorkerSlot, Double> temp=new HashMap<>();
                if(d.containsKey(slot1))
                    temp=d.get(slot1);
                temp.put(slot2,transferTime);
                d.put(slot1,temp);

            }
        }
        return d;
    }

    /**
     * 递归调用构造RevocerTime 模型参数
     * @param topology
     * @param component
     * @param w
     * @param layer
     */
    public static void initializeWRecursiveConstruction(TopologyDetails topology, Component component, Map<ExecutorDetails, Map<ExecutorDetails, Double>> w, int layer){
        layer++;//层数+1
        List<String> parentsId = component.parents;

        //过滤掉根节点 spout
        if(parentsId.size()!=0) {
            double upComponetExces = 0.0;
            for (String parentId : parentsId) {
                Component parent = topology.getComponents().get(parentId);
                upComponetExces += parent.execs.size();
            }
            //以componet为目标节点插入所有权值
            insertWModel(topology, component, upComponetExces * layer, w);
        }

        List<String> childsId = component.children;
        if (childsId.size() == 0) {
            return;
        }

        //递归调用遍历整个topology结构图
        for(String childId :childsId){
            Component child = topology.getComponents().get(childId);
            initializeWRecursiveConstruction(topology,child,w,layer);
        }

    }

    /**
     * 插入到w RecoverTime恢复模型
     * @param topology
     * @param component
     * @param cost
     * @param w
     */
    public static void insertWModel(TopologyDetails topology,Component component, Double cost,Map<ExecutorDetails, Map<ExecutorDetails, Double>> w){
        List<String> parentsId = component.parents;
        for(String parentId :parentsId){
            Component parent = topology.getComponents().get(parentId);
            for(ExecutorDetails parentexecutor :parent.execs)
                for(ExecutorDetails childexecutor :component.execs){
                    Map<ExecutorDetails, Double> temp;
                    if(w.containsKey(parentexecutor))
                        temp=w.get(parentexecutor);
                    else
                        temp=new HashMap<>();
                    temp.put(childexecutor,cost);

                    w.put(parentexecutor,temp);
                }
        }

    }


    //The parameter w denotes the recover time of upstream and downstream executor pairs.
    public static Map<ExecutorDetails, Map<ExecutorDetails, Double>>  initializeW(TopologyDetails topology, List<ExecutorDetails> executors) {
        Map<ExecutorDetails, Map<ExecutorDetails, Double>> w = new HashMap<ExecutorDetails, Map<ExecutorDetails, Double>>();

        Map<String, SpoutSpec> spouts = topology.getTopology().get_spouts();
        for(String spoutId:spouts.keySet()){
            Component spout = topology.getComponents().get(spoutId);
            int layer=0;
            initializeWRecursiveConstruction(topology,spout,w,layer);
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
