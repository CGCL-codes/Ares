package com.basic.benchmark;

import com.basic.benchmark.model.LineItem;
import com.basic.benchmark.model.Orders;

import java.io.BufferedReader;

/**
 * locate com.basic.benchmark
 * Created by 79875 on 2017/10/20.
 */
public class JoinDataAnalysis {
    /**
     * 通过文件构造LineItem模型
     * @param bufferedReader
     * @return
     */
    public static LineItem genderateLineitemData(BufferedReader bufferedReader){
        LineItem lineItem=null;
        try {
            String str="";
            if ((str=bufferedReader.readLine())!=null) {
                String[] splits = str.split("\\|");
                lineItem= new LineItem(splits[0], splits[1], splits[2], splits[3], splits[4], splits[5], splits[6], splits[7], splits[8], splits[9]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return lineItem;
    }

    /**
     * 通过文件构造Orders模型
     * @param bufferedReader
     * @return
     */
    public static Orders genderateOrdersData(BufferedReader bufferedReader){
        Orders orders=null;
        try {
            String str="";
            if ((str=bufferedReader.readLine())!=null){
                String[] splits = str.split("\\|");
                orders=new Orders(splits[0],splits[1],splits[2],splits[3],splits[4],splits[5],splits[6]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return orders;
    }
}
