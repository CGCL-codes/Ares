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

    private static List<WorkerSlot> getAllSlots(Cluster cluster){
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>();
        for (SupervisorDetails supervisor :cluster.getSupervisors().values()) {
            slots.addAll(getAllSlotsSupervisor(cluster,supervisor));
        }

        return slots;
    }

    /**
     * Return all the available slots on this supervisor.
     */
    private static List<WorkerSlot> getAllSlotsSupervisor(Cluster cluster, SupervisorDetails supervisor) {
        Set<Integer> ports = getAllPorts(cluster,supervisor);
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>(ports.size());

        for (Integer port : ports) {
            slots.add(new WorkerSlot(supervisor.getId(), port));
        }
        return slots;
    }

    private static Set<Integer> getAllPorts(Cluster cluster, SupervisorDetails supervisor) {
        Set<Integer> ret = new HashSet<>();
        ret.addAll(cluster.getAssignablePorts(supervisor));
        return ret;
    }

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

        HashMap<WorkerSlot, List<ExecutorDetails>> workerSlotListHashMap = AresUtils.reverseMap(assignment);
        computeCostUtil.initProcessingCostMap(slots,workerSlotListHashMap);

        //The flag indicates whether achieves Nash equilibrium.
        boolean isNashEquilibrium;

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
        for (ExecutorDetails executor : componentExecutors) {
            Random random = new Random();
            int index = random.nextInt(slots.size());
            assignment.put(executor, slots.get(index));
        }

        //The process of best-response dynamics.

        do {
            isNashEquilibrium = true;
            //Make the best-response strategy for each executor by turn.

            for (ExecutorDetails executor : componentExecutors) {
                String currentComponentId = topology.getExecutorToComponent().get(executor);
                Component currentComponent = topology.getComponents().get(currentComponentId);
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
                    Map<WorkerSlot, Double> costExecutorToSlot = new HashMap<WorkerSlot, Double>();
                    for (WorkerSlot slot : slots) {
                        double totalcost=computeCostUtil.totalProcessingCostOfExecutorsOnSlot.get(slot)+computeCostUtil.computeProcessingCost(executor,slot);
                        costExecutorToSlot.put(slot,totalcost);

                        for(ExecutorDetails upExecutor : upstreamExecutors) {
                            WorkerSlot upSlot=assignment.get(upExecutor);
                            computeTransferringAndRecoveryCost(nodeToRack,costExecutorToSlot,upSlot,slot,upExecutor,executor);
                        }
                        for(ExecutorDetails downExecutor : downstreamExecutors) {
                            WorkerSlot downSlot=assignment.get(downExecutor);
                            computeTransferringAndRecoveryCost(nodeToRack, costExecutorToSlot, slot, downSlot, executor, downExecutor);
                        }
                    }

                    //Make the best-response strategy for an executor.
                    double minCost = Double.MAX_VALUE;
                    for (Map.Entry<WorkerSlot, Double> entry : costExecutorToSlot.entrySet()) {
                        if (entry.getValue() < minCost) {
                            minCost = entry.getValue();
                            assignment.put(executor, entry.getKey());
                        }
                    }

                    LOG.info("compentId:"+topology.getExecutorToComponent().get(executor)+" executorId:"+executor.getStartTask()+"  prehost:"+cluster.getHost(preAssignmentWorkSlot.getNodeId())+" perport:"+preAssignmentWorkSlot.getPort()+" nowhost:"+cluster.getHost(assignment.get(executor).getNodeId())+" nowprot:"+assignment.get(executor).getPort());

                    ////////////////////////////////////////update//////////////////////////////////////////////
                    workerSlot = assignment.get(executor);
                    computeCostUtil.updateProcessingCostMap(workerSlot,executor);
                    /////////////////////////////////////////update///////////////////////////////////////////

                    //Check whether achieves Nash equilibrium.
                    if (isNashEquilibrium && assignment.get(executor) != preAssignmentWorkSlot) {
                        isNashEquilibrium = false;
                    }
                }
            }
            LOG.info("");
        } while (!isNashEquilibrium);

        /**
         * 将AckEcutor 随机放置到assigment中
         */
        for (ExecutorDetails executor : ackExecutors) {
            Random random = new Random();
            int index = random.nextInt(slots.size());
            assignment.put(executor, slots.get(index));
        }
        return assignment;
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
        costExecutorToSlot.put(slot,costExecutorToSlot.get(slot) + transferringCost);
        if (nodeToRack.get(slot.getNodeId()).equals(nodeToRack.get(downSlot.getNodeId()))) {
            double recoveryCost=computeCostUtil.computeRecoveryCost(executor,downExecutor);
            costExecutorToSlot.put(slot,costExecutorToSlot.get(slot)+ recoveryCost);
        }
    }

    private static Map<WorkerSlot, List<ExecutorDetails>> getAliveAssignedWorkerSlotExecutors(Cluster cluster, String topologyId) {
        SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyId);
        Map<ExecutorDetails, WorkerSlot> executorToSlot = null;
        if (existingAssignment != null) {
            executorToSlot = existingAssignment.getExecutorToSlot();
        }

        return AresUtils.reverseMap(executorToSlot);
    }


    private static Map<ExecutorDetails, WorkerSlot> scheduleTopologyWithGame(TopologyDetails topology, Cluster cluster) {
        LOG.info("start scheduleTopologyWithGame................................");

        Map<ExecutorDetails, WorkerSlot> reassignment = new HashMap<ExecutorDetails, WorkerSlot>();

        //TODO Storm 每10s钟 调用schedule方法
        List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
        List<WorkerSlot> allSlots = getAllSlots(cluster);

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

        LOG.info("reassignSlots size:"+ reassignSlots.size()+" reassignExecutors size:"+executors.size()+" allexecutors size: "+allexecutors.size());
        if(executors.size() != allexecutors.size()){
            return reassignment;
        }

        computeCostUtil=ComputeCostUtil.getInstance(topology,cluster);
        computeCostUtil.initPara();

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
