package com.basic.core.util;

import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;

import java.util.*;

/**
 * locate com.basic.core.util
 * Created by 79875 on 2017/10/4.
 */
public class AresUtils {
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
        if (availableSlots != null && availableSlots.size() > 0) {
            // group by node
            Map<String, List<WorkerSlot>> slotGroups = new TreeMap<String, List<WorkerSlot>>();
            for (WorkerSlot slot : availableSlots) {
                String node = slot.getNodeId();
                List<WorkerSlot> slots = null;
                if(slotGroups.containsKey(node)){
                    slots = slotGroups.get(node);
                }else{
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
}
