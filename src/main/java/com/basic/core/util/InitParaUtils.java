package com.basic.core.util;


import com.basic.core.model.ComponentPair;
import com.basic.core.model.NodePair;
import com.basic.util.PropertiesUtil;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * locate com.basic.util
 * Created by 79875 on 2017/9/25.
 * W1*计算延迟 + W2*恢复时间
 * InitParaUtils 工具包
 */
public class InitParaUtils {
    private static final Logger LOG = LoggerFactory.getLogger(InitParaUtils.class);
    //The parameter W1 and W2 denote the weight of the stream` response time and the stream recovery time, respectively;
    // public static double W1, W2;
    public static double W1 = 0.9, W2 = 0.1;

    //The parameter q denotes the computation cost for executors to process a single tuple.
    public static Map<String, Double> initializeQ(TopologyDetails topology) {
        PropertiesUtil.init("/componentcost.properties");
        Map<String, Component> components = topology.getComponents();
        Map<String, Double> q = new HashMap<>();
        for(String ComponentId : components.keySet()){
            q.put(ComponentId, Double.valueOf(PropertiesUtil.getProperties(ComponentId)));
        }
        return q;
    }

    //The parameter lambda denotes the data processing time for slots to process a single computation cost.
    public static Map<String, Double> initializeLambda(Cluster cluster) {
        //*Double.valueOf(PropertiesUtil.getProperties(componentId))
        PropertiesUtil.init("/nodecomputecost.properties");
        Map<String, Double> lambda = new HashMap<>();
        Map<String, SupervisorDetails> supervisors = cluster.getSupervisors();
        for (String nodeId : supervisors.keySet()) {
            SupervisorDetails supervisorDetails = supervisors.get(nodeId);
            lambda.put(supervisorDetails.getHost(),Double.valueOf(PropertiesUtil.getProperties(supervisorDetails.getHost())));
        }
        return lambda;
    }

    //The parameter d denotes the data transferring time of node pairs.
    public static  Map<NodePair, Double> initializeD(Cluster cluster) {
        PropertiesUtil.init("/nodetransferpair.properties");
        Map<NodePair, Double> d = new HashMap<>();
        Map<String, SupervisorDetails> supervisors = cluster.getSupervisors();

        for (String upnodeId : supervisors.keySet())
            for(String downnodeId:supervisors.keySet()){
                SupervisorDetails upnode=supervisors.get(upnodeId);
                SupervisorDetails downnode=supervisors.get(downnodeId);
                Double transferTime= Double.valueOf(PropertiesUtil.getProperties(upnode.getHost()+","+downnode.getHost()));
                d.put(new NodePair(upnode.getHost(),downnode.getHost()),transferTime);
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
//    public static void initializeWRecursiveConstruction(TopologyDetails topology, Component component, Map<ComponentPair, Double> w, int layer){
//        layer++;//层数+1
//        List<String> parentsId = component.parents;
//
//        //过滤掉根节点 spout
//        if(parentsId.size()!=0) {
//            double upComponetExces = 0.0;
//            for (String parentId : parentsId) {
//                Component parent = topology.getComponents().get(parentId);
//                upComponetExces += parent.execs.size();
//            }
//            //以componet为目标节点插入所有权值
//            insertWModel(topology, component, upComponetExces * layer, w);
//        }
//
//        List<String> childsId = component.children;
//        if (childsId.size() == 0) {
//            return;
//        }
//
//        //递归调用遍历整个topology结构图
//        for(String childId :childsId){
//            Component child = topology.getComponents().get(childId);
//            initializeWRecursiveConstruction(topology,child,w,layer);
//        }
//
//    }

    /**
     * 插入到w RecoverTime恢复模型
     * @param topology
     * @param component
     * @param cost
     * @param w
     */
    public static void insertWModel(TopologyDetails topology, Component component, Double cost, Map<ComponentPair, Double> w){
        List<String> parentsId = component.parents;
        for(String parentId :parentsId){
            Component parent = topology.getComponents().get(parentId);
            w.put(new ComponentPair(parent,component),cost);
        }

    }


    /**
     * Topology 恢复时间权值权值
     * computeCost需要使用Alpha
     * Alpha经过当前Componet的路径条数/Toplogy总条数*W1
     * @param topology
     * @return
     */
    public static Map<ComponentPair, Double>  initializeW(TopologyDetails topology) {
        Map<ComponentPair, Double> w = new HashMap<>();

        for(String compentId:topology.getComponents().keySet()){
            Component component = topology.getComponents().get(compentId);
            int layer=AresUtils.getLayer(topology,component);
            List<String> parentsId = component.parents;
            double upComponetExces = 0.0;
            for (String parentId : parentsId) {
                Component parent = topology.getComponents().get(parentId);
                upComponetExces += parent.execs.size();
            }
            //以componet为目标节点插入所有权值
            insertWModel(topology, component, upComponetExces * layer, w);
        }
        return w;
    }

    /**
     * 递归调用得到当前孩子路径数量，然后计算总路径数量
     * @param topology
     * @param component
     * @return
     */
    public static double getChildsPathsNumber(TopologyDetails topology, Component component){
        double temp=0.0;
        List<String> childrensId = component.children;
        if(childrensId.size()==0)
            return 1;
        for(String childId :childrensId){
            Component child = topology.getComponents().get(childId);
            temp+=child.execs.size()*getChildsPathsNumber(topology,child);
        }
        return temp;
    }

    /**
     * 递归调用得到当前父母路径数量，然后计算总路径数量
     * @param topology
     * @param component
     * @return
     */
    public static double getParentsPathsNumber(TopologyDetails topology, Component component){
        double temp=0.0;
        List<String> parentsId = component.parents;
        if(parentsId.size()==0)
            return 1;
        for(String parentId :parentsId){
            Component parent = topology.getComponents().get(parentId);
            temp+=parent.execs.size()*getParentsPathsNumber(topology,parent);
        }
        return temp;
    }

    /**
     * Topology 计算权值
     * computeCost需要使用Alpha
     * Alpha经过当前Componet的路径条数/Toplogy总条数*W1
     * @param topology
     * @return
     */
    public static Map<String, Double> initializeAlpha(TopologyDetails topology) {
        Map<String, Double> alpha = new HashMap<>();

        //Calculate the total number of paths
        double allPathLength=0.0;

        Map<String, SpoutSpec> spouts = topology.getTopology().get_spouts();
        for(String spoutId:spouts.keySet()){
            Component spout = topology.getComponents().get(spoutId);
            allPathLength+=spout.execs.size()*getChildsPathsNumber(topology,spout);
        }

        Map<String, Component> components = topology.getComponents();
        for(String componentId : components.keySet()){
            Component component = components.get(componentId);
            if(component==null){
                continue;
            }
            double childPathNumber=getChildsPathsNumber(topology,component);
            double parentPathNumber=getParentsPathsNumber(topology,component);
            double currentExecutorPathNumber=parentPathNumber*childPathNumber;
            alpha.put(componentId, W1*(currentExecutorPathNumber/allPathLength));
        }
        return alpha;
    }

    /**
     ** Topology 传输时间权值
     *  TransferringCost需要使用Beta
     *  Beta经过当前ComponentPair的路径条数/Toplogy总条数*W1
     * @param topology
     * @return
     */
    public static Map<ComponentPair, Double> initializeBeta(TopologyDetails topology) {
        Map<ComponentPair, Double> beta = new HashMap<>();
        //Calculate the total number of paths
        double allPathLength=0.0;

        Map<String, SpoutSpec> spouts = topology.getTopology().get_spouts();
        for(String spoutId:spouts.keySet()){
            Component spout = topology.getComponents().get(spoutId);
            allPathLength+=spout.execs.size()*getChildsPathsNumber(topology,spout);
        }

        Map<String, Component> components = topology.getComponents();
        for(String componentId : components.keySet()){
            Component component = components.get(componentId);
            if (component == null || component.children.size()==0) {
                continue;
            }
            Iterator<String> childcomponentIdIterator = component.children.iterator();
            while (childcomponentIdIterator.hasNext()){
                String childcomponentId = childcomponentIdIterator.next();
                Component childcomponent = topology.getComponents().get(childcomponentId);
                    double parentPathNumber=1;
                    if(component.parents.size()!=0){
                        parentPathNumber=getParentsPathsNumber(topology,component);
                    }

                    double childPathNumber=getChildsPathsNumber(topology,childcomponent);
                    //LOG.info("parentPathNumber:"+parentPathNumber+" childPathNumber:"+childPathNumber);
                    double currentPathNumber=parentPathNumber*childPathNumber;

                    double cost=W1*currentPathNumber/allPathLength;
                    beta.put(new ComponentPair(component,childcomponent),cost);
            }
        }
        return beta;
    }

    /**
     *  Topology 恢复时间权值
     *  RecoveryCost需要使用Gama
     *  Gama W2/当前集群环境机架个数
     * @param topology
     * @param cluster
     * @return
     */
    public static Map<ComponentPair, Double>  initializeGamma(TopologyDetails topology, Cluster cluster) {
        Map<String, List<String>> networkTopography = cluster.getNetworkTopography();
        Map<ComponentPair, Double> gamma = new HashMap<>();
        Map<String, Component> components = topology.getComponents();

        for(String componentId : components.keySet()){
            Component component = components.get(componentId);
            if (component == null || component.children.size()==0) {
                continue;
            }
            Iterator<String> childcomponentIdIterator = component.children.iterator();
            while (childcomponentIdIterator.hasNext()){
                String childcomponentId = childcomponentIdIterator.next();
                Component childcomponent = topology.getComponents().get(childcomponentId);
                gamma.put(new ComponentPair(component,childcomponent),W2/networkTopography.size());
            }
        }
        return gamma;
    }
}
