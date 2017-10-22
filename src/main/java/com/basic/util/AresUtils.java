package com.basic.util;

import org.apache.storm.scheduler.*;
import org.apache.storm.scheduler.resource.Component;

import java.util.*;

/**
 * locate com.basic.core.util
 * Created by 79875 on 2017/10/4.
 */
public class AresUtils {
    public static List<WorkerSlot> getAllSlots(Cluster cluster){
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>();
        for (SupervisorDetails supervisor :cluster.getSupervisors().values()) {
            slots.addAll(getAllSlotsSupervisor(cluster,supervisor));
        }

        return slots;
    }

    /**
     * Return all the available slots on this supervisor.
     */
    public static List<WorkerSlot> getAllSlotsSupervisor(Cluster cluster, SupervisorDetails supervisor) {
        Set<Integer> ports = getAllPorts(cluster,supervisor);
        List<WorkerSlot> slots = new ArrayList<WorkerSlot>(ports.size());

        for (Integer port : ports) {
            slots.add(new WorkerSlot(supervisor.getId(), port));
        }
        return slots;
    }

    public static Set<Integer> getAllPorts(Cluster cluster, SupervisorDetails supervisor) {
        Set<Integer> ret = new HashSet<>();
        ret.addAll(cluster.getAssignablePorts(supervisor));
        return ret;
    }

    /**
     * 得到当前Componet在topology中的层数
     * @param topology
     * @param component
     * @return
     */
    public static int getLayer(TopologyDetails topology,Component component){
        if(component.parents.size()==0)
            return 1;

        List<String> parentsId = component.parents;
        List<Integer> layeList=new ArrayList<>();
        for(String parentid :parentsId){
            layeList.add(getLayer(topology,topology.getComponents().get(parentid)));
        }
        Collections.sort(layeList);
        return layeList.get(0)+1;
    }


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

    public static boolean isComponentAcker(TopologyDetails topology, ExecutorDetails executor){
        String currentComponentId = topology.getExecutorToComponent().get(executor);
        return topology.getComponents().get(currentComponentId)==null;
    }

    /**
     * 对availableSlots进行排序 使它使用EvenSchedule方法分配Executor
     * @param availableSlots
     * @return
     */
    public static List<WorkerSlot> sortSlots(List<WorkerSlot> availableSlots) {
        //For example, we have a three nodes(supervisor1, supervisor2, supervisor3) cluster:
        //slots before sort:
        //supervisor1:6700, supervisor1:6701,
        //supervisor2:6700, supervisor2:6701, supervisor2:6702,
        //supervisor3:6700, supervisor3:6703, supervisor3:6702, supervisor3:6701
        //slots after sort:
        //supervisor3:6700, supervisor2:6700, supervisor1:6700,
        //supervisor3:6701, supervisor2:6701, supervisor1:6701,
        //supervisor3:6702, supervisor2:6702,
        //supervisor3:6703

        if (availableSlots != null && availableSlots.size() > 0) {
            // group by node
            Map<String, List<WorkerSlot>> slotGroups = new TreeMap<>();
            for (WorkerSlot slot : availableSlots) {
                String node = slot.getNodeId();
                List<WorkerSlot> slots = null;
                if (slotGroups.containsKey(node)) {
                    slots = slotGroups.get(node);
                } else {
                    slots = new ArrayList<WorkerSlot>();
                    slotGroups.put(node, slots);
                }
                slots.add(slot);
            }

            // sort by port: from small to large
            for (List<WorkerSlot> slots : slotGroups.values()) {
                Collections.sort(slots, new Comparator<WorkerSlot>() {
                    @Override
                    public int compare(WorkerSlot o1, WorkerSlot o2) {
                        return o1.getPort() - o2.getPort();
                    }
                });
            }

            // sort by available slots size: from large to small
            List<List<WorkerSlot>> list = new ArrayList<List<WorkerSlot>>(slotGroups.values());
            Collections.sort(list, new Comparator<List<WorkerSlot>>() {
                @Override
                public int compare(List<WorkerSlot> o1, List<WorkerSlot> o2) {
                    return o2.size() - o1.size();
                }
            });

            return interleaveAll(list);
        }

        return null;
    }

    /**
     * interleaveAll 这个函数的功能是递归从这个list中每次取每个子list的第一个元素形成最终的assign slots list
     * @param nodeList
     * @param <T>
     * @return
     */
    public static <T> List<T> interleaveAll(List<List<T>> nodeList) {
        if (nodeList != null && nodeList.size() > 0) {
            List<T> first = new ArrayList<T>();
            List<List<T>> rest = new ArrayList<List<T>>();
            for (List<T> node : nodeList) {
                if (node != null && node.size() > 0) {
                    first.add(node.get(0));
                    rest.add(node.subList(1, node.size())); //List<E> subList(int fromIndex, int toIndex);
                }
            }
            List<T> interleaveRest = interleaveAll(rest);
            if (interleaveRest != null) {
                first.addAll(interleaveRest);
            }
            return first;
        }
        return null;
    }

    public static void waitForTimeMillis(long delayTime){
        long startTime = System.currentTimeMillis();
        while (true){
            long endTime = System.currentTimeMillis();
            if(endTime-startTime>delayTime)
                break;
        }
    }
}
