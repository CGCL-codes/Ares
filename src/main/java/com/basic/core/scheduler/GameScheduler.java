package com.basic.core.scheduler;

import com.basic.core.util.AresUtils;
import com.basic.core.util.ComputeCostUtil;
import com.google.common.collect.Sets;
import org.apache.storm.scheduler.*;
import org.apache.storm.scheduler.resource.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GameScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(GameScheduler.class);

    private static ComputeCostUtil computeCostUtil;


    private static Map<String, String> getNodeToRack(Cluster cluster){
        //Initialize the network topology.
        Map<String, List<String>> networkTopography = cluster.getNetworkTopography();
        Map<String, String> nodeToRack = new HashMap<String, String>();
        for (Map.Entry<String, List<String>> entry : networkTopography.entrySet()) {
            String rack = entry.getKey();
            List<String> nodes = entry.getValue();
            for (String node : nodes) {
                List<SupervisorDetails> supervisorsByHost = cluster.getSupervisorsByHost(node);
                nodeToRack.put(supervisorsByHost.get(0).getId(), rack);
            }
        }
        return nodeToRack;
    }

    private static Map<ExecutorDetails, WorkerSlot> gameScheduling(TopologyDetails topology, Cluster cluster, List<ExecutorDetails> allExecutors, List<ExecutorDetails> executors, List<WorkerSlot> slots) {
        LOG.info("gameScheduling................................");

        //Assign an executor to a slot randomly.
        Map<ExecutorDetails, WorkerSlot> assignment = new HashMap<ExecutorDetails, WorkerSlot>();
        Map<String, String> nodeToRack = getNodeToRack(cluster);

        List<ExecutorDetails> componentExecutors=new ArrayList<>();
        List<ExecutorDetails> ackExecutors=new ArrayList<>();

        //The flag indicates whether achieves Nash equilibrium.
        boolean isNashEquilibrium;

        LOG.info("reassignExecutors................................");
        for(ExecutorDetails executor:executors){
            LOG.info("compentId:"+topology.getExecutorToComponent().get(executor)+" executorId:"+executor.getStartTask());
        }
        LOG.info("");

        /**
         * 初始化componentExecutor 和 ackExecutors
         */
        for (ExecutorDetails executor : executors) {
            if(AresUtils.isComponentAcker(topology,executor))
                ackExecutors.add(executor);
            else
                componentExecutors.add(executor);
        }

        /**
         * 首先随机放置 初始化
         */
        //randomAssignment(assignment, componentExecutors, slots);
        evenSortAssignment(cluster,topology,assignment,componentExecutors,slots);

        LOG.info("First evenSortAssignment................................");
        for(ExecutorDetails executor:assignment.keySet()){
            WorkerSlot slot = assignment.get(executor);
            LOG.info("compentId:"+topology.getExecutorToComponent().get(executor)+" executorId:"+executor.getStartTask()+" host:"+cluster.getHost(slot.getNodeId())+" port:"+slot.getPort());
        }
        LOG.info("");

        /**
         * 重新恢复Schedule
         */
        if(executors.size() != allExecutors.size()){
            randomAssignment(assignment,ackExecutors,slots);
            return assignment;
        }

        computeCostUtil.initProcessingCostMap(slots,assignment);
        double evenUtilityCost = computeCostUtil.computeUtilityCost(assignment);
        LOG.info("EvenUtilityCost: "+evenUtilityCost);

        do {
            isNashEquilibrium = true;
            //Make the best-response strategy for each executor by turn.

            for (ExecutorDetails executor : componentExecutors) {
                String currentComponentId = topology.getExecutorToComponent().get(executor);
                Component currentComponent = topology.getComponents().get(currentComponentId);

//                LOG.info("host\tport\ttotalProcessOnslot\tContainTaskNum");
//                for(WorkerSlot slot:computeCostUtil.totalProcessingCostOfExecutorsOnSlot.keySet()){
//                    LOG.info(cluster.getHost(slot.getNodeId())+"\t"+slot.getPort()+"\t"+computeCostUtil.totalProcessingCostOfExecutorsOnSlot.get(slot)+"\t"+computeCostUtil.slotContainTaskNum.get(slot));
//                }
//                LOG.info("");

                if(currentComponent!=null){
                    //过滤掉__acker的ExecutorDetails

                    ////////////////////////////////////////delete//////////////////////////////////////////////
                    WorkerSlot workerSlot = assignment.get(executor);
                    computeCostUtil.deleteProcessingCostMap(workerSlot,executor);
                    ////////////////////////////////////////delete//////////////////////////////////////////////

                    //Initialize the list of upstream and downstream executors for current executor.
                    List<ExecutorDetails> upstreamExecutors = new ArrayList<>();
                    //LOG.info("currentComponent: "+String.valueOf(currentComponent));
                    for (String parentId : currentComponent.parents) {
                        List<ExecutorDetails> parentExecutors = topology.getComponents().get(parentId).execs;
                        upstreamExecutors.addAll(parentExecutors);
                    }
                    List<ExecutorDetails> downstreamExecutors = new ArrayList<>();
                    for (String childrenId : currentComponent.children) {
                        downstreamExecutors.addAll(topology.getComponents().get(childrenId).execs);
                    }

                    //Store the previous assignment of an executor for later check of Nash equilibrium.
                    WorkerSlot preAssignmentWorkSlot = assignment.get(executor);

                    //Initialize the costs of assigning an executor to different slots.
                    LOG.info("compentId\texecutorId\thost\tport\ttotalCost\tcomputeCost\ttransferringCost\trecoveryCost");
                    Map<WorkerSlot, Double> costExecutorToSlot = new HashMap<WorkerSlot, Double>();
                    for (WorkerSlot slot : slots) {
                        double totalcost=0.0;
                        double computeCost=0.0;
                        double transferringCost=0.0;
                        double recoveryCost=0.0;

                        computeCost = computeCostUtil.computeProcessingCost(executor,slot);
//                       LOG.info(computeCostUtil.totalProcessingCostOfExecutorsOnSlot.get(slot)+" "+computeCostUtil.computeProcessingCost(executor,slot));
                        for(ExecutorDetails upExecutor : upstreamExecutors) {
                            WorkerSlot upSlot=assignment.get(upExecutor);
                            transferringCost += computeCostUtil.computeTransferringCost(upExecutor,executor,upSlot,slot)/2;
                            if (nodeToRack.get(slot.getNodeId()).equals(nodeToRack.get(slot.getNodeId()))) {
                                recoveryCost += computeCostUtil.computeRecoveryCost(upExecutor,executor)/2;
                            }
                        }

                        for(ExecutorDetails downExecutor : downstreamExecutors) {
                            WorkerSlot downSlot=assignment.get(downExecutor);
                            transferringCost += computeCostUtil.computeTransferringCost(executor,downExecutor,slot,downSlot)/2;
                            if (nodeToRack.get(slot.getNodeId()).equals(nodeToRack.get(downSlot.getNodeId()))) {
                                recoveryCost += computeCostUtil.computeRecoveryCost(executor,downExecutor)/2;
                            }
                        }
                        totalcost= transferringCost + recoveryCost +computeCost;
                        costExecutorToSlot.put(slot,totalcost);
                        LOG.info(topology.getExecutorToComponent().get(executor)+"\t"+executor.getStartTask()+"\t"+cluster.getHost(slot.getNodeId())+"\t"+slot.getPort()+"\t"+totalcost+"\t"+computeCost+"\t"+transferringCost+"\t"+recoveryCost);
                    }

                    //Make the best-response strategy for an executor.
                    double minCost = Double.MAX_VALUE;
                    for (WorkerSlot slot : costExecutorToSlot.keySet()) {
                        Double cost = costExecutorToSlot.get(slot);
                        if (cost < minCost) {
                            minCost = cost;
                            assignment.put(executor, slot);
                        }
                    }
                    LOG.info("compentId\texecutorId\tprehost\tnowhost\tnowoort");
                    LOG.info(topology.getExecutorToComponent().get(executor)+"\t"+executor.getStartTask()+"\t"+cluster.getHost(preAssignmentWorkSlot.getNodeId())+"\t"+preAssignmentWorkSlot.getPort()+"\t"+cluster.getHost(assignment.get(executor).getNodeId())+"\t"+assignment.get(executor).getPort());

                    ////////////////////////////////////////update//////////////////////////////////////////////
                    workerSlot = assignment.get(executor);
                    computeCostUtil.updateProcessingCostMap(workerSlot,executor);
                    /////////////////////////////////////////update///////////////////////////////////////////

                    //Check whether achieves Nash equilibrium.
                    if (isNashEquilibrium && assignment.get(executor) != preAssignmentWorkSlot) {
                        isNashEquilibrium = false;
                    }
                }
                LOG.info("-------------------------------------------------------------");
            }
            LOG.info("----------------------------next Rank--------------------------------");
        } while (!isNashEquilibrium);

        computeCostUtil.initProcessingCostMap(slots,assignment);
        double gameUtilityCost = computeCostUtil.computeUtilityCost(assignment);
        LOG.info("GameUtilityCost: "+gameUtilityCost);

        //打散在同一个Node节点下的Executor
        randomNodeSlotAssignment(topology,cluster,assignment);

        /**
         * 将AckEcutor 随机放置到assigment中
         */
        randomAssignment(assignment, ackExecutors, slots);
        return assignment;
    }

    private static void randomNodeSlotAssignment(TopologyDetails topology, Cluster cluster, Map<ExecutorDetails, WorkerSlot> assignment) {
        Map<ExecutorDetails, SupervisorDetails> nodeSlotAssignment=new HashMap<>();
        for(ExecutorDetails executor : assignment.keySet()){
            WorkerSlot slot=assignment.get(executor);
            SupervisorDetails supervisorDetails = cluster.getSupervisors().get(slot.getNodeId());
            nodeSlotAssignment.put(executor,supervisorDetails);
        }
        HashMap<SupervisorDetails, List<ExecutorDetails>> supervisorDetailsListHashMap = AresUtils.reverseMap(nodeSlotAssignment);
        assignment.clear();
        for(SupervisorDetails supervisorDetails :supervisorDetailsListHashMap.keySet()){
            List<ExecutorDetails> executorDetails = supervisorDetailsListHashMap.get(supervisorDetails);
            List<WorkerSlot> allSlotsSupervisor = AresUtils.getAllSlotsSupervisor(cluster, supervisorDetails);
            for(int i=0;i<executorDetails.size();i++){
                assignment.put(executorDetails.get(i),allSlotsSupervisor.get(i%allSlotsSupervisor.size()));
            }
        }
        LOG.info("reassignment:"+ assignment+"\n");
        for(ExecutorDetails executor:assignment.keySet()){
            WorkerSlot slot = assignment.get(executor);
            LOG.info("compentId:"+topology.getExecutorToComponent().get(executor)+" executorId:"+executor.getStartTask()+" host:"+cluster.getHost(slot.getNodeId())+" port:"+slot.getPort());
        }
    }

    /**
     * 随机分配componentExecutors到slots上面去
     * @param assignment
     * @param Executors
     * @param slots
     */
    private static void randomAssignment(Map<ExecutorDetails, WorkerSlot> assignment, List<ExecutorDetails> Executors, List<WorkerSlot> slots) {
        for (ExecutorDetails executor : Executors) {
            Random random = new Random();
            int index = random.nextInt(slots.size());
            assignment.put(executor, slots.get(index));
        }
    }

    private static void evenSortAssignment(Cluster cluster, TopologyDetails topology, Map<ExecutorDetails, WorkerSlot> assignment, List<ExecutorDetails> Executors, List<WorkerSlot> slots){
//        Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned = getAliveAssignedWorkerSlotExecutors(cluster, topology.getId());
//        int totalSlotsToUse = Math.min(topology.getNumWorkers(), slots.size() + aliveAssigned.size());

        List<WorkerSlot> sortedList = AresUtils.sortSlots(slots);

        //allow requesting slots number bigger than available slots
//        int toIndex = (totalSlotsToUse - aliveAssigned.size()) > sortedList.size() ? sortedList.size() : (totalSlotsToUse - aliveAssigned.size());
        List<WorkerSlot> reassignSlots = sortedList;

        for (int i = 0; i < Executors.size(); i++) {
            assignment.put(Executors.get(i), reassignSlots.get(i % reassignSlots.size()));
        }

    }

    /**
     * 计算传输时间和恢复时间权值
     * @param nodeToRack
     * @param costExecutorToSlot
     * @param slot
     * @param downSlot
     * @param executor
     * @param downExecutor
     */
    private static void computeTransferringAndRecoveryCost(Map<String, String> nodeToRack, Map<WorkerSlot, Double> costExecutorToSlot, WorkerSlot slot, WorkerSlot downSlot, ExecutorDetails executor, ExecutorDetails downExecutor) {
        double transferringCost = computeCostUtil.computeTransferringCost(executor,downExecutor,slot,downSlot);
        if(costExecutorToSlot.get(slot) == null || slot==null){
            int a;
        }
        costExecutorToSlot.put(slot,costExecutorToSlot.get(slot) + transferringCost);
//        if (nodeToRack.get(slot.getNodeId()).equals(nodeToRack.get(downSlot.getNodeId()))) {
//            double recoveryCost=computeCostUtil.computeRecoveryCost(executor,downExecutor);
//            costExecutorToSlot.put(slot,costExecutorToSlot.get(slot)+ recoveryCost);
//        }
    }

    private static Map<WorkerSlot, List<ExecutorDetails>> getAliveAssignedWorkerSlotExecutors(Cluster cluster, String topologyId) {
        SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyId);
        Map<ExecutorDetails, WorkerSlot> executorToSlot = null;
        if (existingAssignment != null) {
            executorToSlot = existingAssignment.getExecutorToSlot();
        }

        return AresUtils.reverseMap(executorToSlot);
    }


    private static Map<ExecutorDetails, WorkerSlot> scheduleTopologyWithGame(final TopologyDetails topology, Cluster cluster) {
        LOG.info("start scheduleTopologyWithGame................................");

        Map<ExecutorDetails, WorkerSlot> reassignment = new HashMap<ExecutorDetails, WorkerSlot>();

        //TODO Storm 每10s钟 调用schedule方法
        List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
        List<WorkerSlot> allSlots = AresUtils.getAllSlots(cluster);

        LOG.info("GameScheldueing AvaliableWorkSlot................................");
        for(WorkerSlot slot:availableSlots){
            LOG.info("workSlot host:"+cluster.getHost(slot.getNodeId())+" port:"+slot.getPort());
        }

        LOG.info("GameScheldueing AllWorkSlot................................");
        for(WorkerSlot slot:allSlots){
            LOG.info("workSlot host:"+cluster.getHost(slot.getNodeId())+" port:"+slot.getPort());
        }

        Set<ExecutorDetails> allExecutors = (Set<ExecutorDetails>) topology.getExecutors();
        Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned = getAliveAssignedWorkerSlotExecutors(cluster, topology.getId());
        //int totalSlotsToUse = Math.min(topology.getNumWorkers(), availableSlots.size() + aliveAssigned.size());

        if (availableSlots == null) {
            LOG.error("No available slots for topology: {}", topology.getName());
            return new HashMap<ExecutorDetails, WorkerSlot>();
        }

        List<WorkerSlot> reassignSlots = availableSlots;

        Set<ExecutorDetails> aliveExecutors = new HashSet<ExecutorDetails>();
        for (List<ExecutorDetails> list : aliveAssigned.values()) {
            aliveExecutors.addAll(list);
        }
        Set<ExecutorDetails> reassignExecutors = Sets.difference(allExecutors, aliveExecutors);

        if (reassignSlots.size() == 0) {
            return reassignment;
        }

        List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>(reassignExecutors);
        List<ExecutorDetails> allexecutors = new ArrayList<ExecutorDetails>(allExecutors);

        LOG.info("reassignSlots size:"+ reassignSlots.size()+" reassignExecutors size:"+executors.size()+" allexecutors size: "+allexecutors.size()+"\n");

        computeCostUtil=ComputeCostUtil.getInstance(topology,cluster);
        computeCostUtil.initPara();

        Collections.sort(executors, new Comparator<ExecutorDetails>() {
            @Override
            public int compare(ExecutorDetails o1, ExecutorDetails o2) {
                Component component1 = topology.getComponents().get(o1);
                Component component2 = topology.getComponents().get(o2);
                //升序排序
                return AresUtils.getLayer(topology,component2)-AresUtils.getLayer(topology,component1);
            }
        });

        reassignment = gameScheduling(topology, cluster,allexecutors, executors, reassignSlots);

        LOG.info("reassignment:"+ reassignment+"\n");
        for(ExecutorDetails executor:reassignment.keySet()){
            WorkerSlot slot = reassignment.get(executor);
            LOG.info("compentId:"+topology.getExecutorToComponent().get(executor)+" executorId:"+executor.getStartTask()+" host:"+cluster.getHost(slot.getNodeId())+" port:"+slot.getPort());
        }

        if (reassignment.size() != 0) {
            LOG.info("Available slots: {}", availableSlots.toString());
        }
        return reassignment;
    }

    public static void scheduleTopologiesWithGame(Topologies topologies, Cluster cluster) {
        for (TopologyDetails topology : cluster.needsSchedulingTopologies(topologies)) {
            String topologyId = topology.getId();
            Map<ExecutorDetails, WorkerSlot> newAssignment = scheduleTopologyWithGame(topology, cluster);
            Map<WorkerSlot, List<ExecutorDetails>> nodePortToExecutors = AresUtils.reverseMap(newAssignment);

            for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : nodePortToExecutors.entrySet()) {
                WorkerSlot nodePort = entry.getKey();
                List<ExecutorDetails> executors = entry.getValue();
                cluster.assign(nodePort, topologyId, executors);
            }
        }
    }

    @Override
    public void prepare(Map conf) {

    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        scheduleTopologiesWithGame(topologies, cluster);
    }


    public Map<String, Object> config() {
        return new HashMap<>();
    }

}
