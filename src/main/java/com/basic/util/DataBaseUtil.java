package com.basic.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;


/**
 * Created by 79875 on 2017/1/9.
 */
public class DataBaseUtil {

    private static Logger logger= LoggerFactory.getLogger(DataBaseUtil.class);

    public static Connection conn;//创建用于连接数据库的Connection对象
    /**
     * 插入数据到tupleCount表中
     * @param time
     * @param tuplecount
     */
    public static void insertTupleCount(Timestamp time,Long tuplecount){
        try {
            conn=JdbcPool.getConnection();
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_tuplecount(time,tuplecount)"
                    + " VALUES (?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setTimestamp(1,time);
            preparedStatement.setLong(2,tuplecount);
            int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_tuplecount (time,tuplecount) values"+time+" "+tuplecount);
            JdbcPool.release(conn,preparedStatement,null);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    /**
     * 插入数据到spouttupleCount表中
     * @param time
     * @param bytecount
     */
    public static void insertHdfsByteCount(Timestamp time,Long bytecount){
        try {
            conn=JdbcPool.getConnection();
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_hdfsbytecount(time,bytecount)"
                    + " VALUES (?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setTimestamp(1,time);
            preparedStatement.setLong(2,bytecount);
            int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_hdfsbytecount (time,bytecount) values"+time+" "+bytecount);
            JdbcPool.release(conn,preparedStatement,null);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    /**
     * 插入数据到tupleLatency表中
     * @param time
     * @param latency
     */
    public static void insertTupleLatency(Timestamp time,Long latency){
        try {
            conn=JdbcPool.getConnection();
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_tuplelatency(time,tuplelatency)"
                    + " VALUES (?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setTimestamp(1,time);
            preparedStatement.setLong(2,latency);
            int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_tuplelatency (time,tuplecount) values"+time+" "+latency);
            JdbcPool.release(conn,preparedStatement,null);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    /**
     * 插入数据到wordcount表中
     * @param time
     * @param word
     * @param count
     */
    public static void insertAresWordCount(Timestamp time,String word,Long count,Integer number){
        try {
            conn=JdbcPool.getConnection();
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_areswordcount(time,word,count,number)"
                    + " VALUES (?,?,?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setTimestamp(1,time);
            preparedStatement.setString(2,word);
            preparedStatement.setLong(3,count);
            preparedStatement.setInt(4,number);
            int num = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_wordcount (time,word,count) values"+time+" "+word+" "+count);
            JdbcPool.release(conn,preparedStatement,null);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    /**
     * 插入数据到tupleCount表中
     * @param time
     * @param tuplecount
     */
    public static void insertAresTupleCount(Timestamp time,Long tuplecount,int taskid){
        try {
            conn=JdbcPool.getConnection();
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_arestuplecount(time,tuplecount,taskid)"
                    + " VALUES (?,?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setTimestamp(1,time);
            preparedStatement.setLong(2,tuplecount);
            preparedStatement.setInt(3,taskid);
            int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_tuplecount (time,tuplecount) values"+time+" "+tuplecount);
            JdbcPool.release(conn,preparedStatement,null);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    /**
     * 插入数据到spouttupleCount表中
     * @param time
     * @param tuplecount
     */
    public static void insertAresSpoutCount(Timestamp time,Long tuplecount,int taskid){
        try {
            conn=JdbcPool.getConnection();
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_aresspoutcount(time,tuplecount,taskid)"
                    + " VALUES (?,?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setTimestamp(1,time);
            preparedStatement.setLong(2,tuplecount);
            preparedStatement.setInt(3,taskid);
            int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_spouttuplecount (time,tuplecount) values"+time+" "+tuplecount);
            JdbcPool.release(conn,preparedStatement,null);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public static void insertDefaultSpoutCount(Timestamp time,Long tuplecount,int taskid){
        try {
            conn=JdbcPool.getConnection();
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_defaultspoutcount(time,tuplecount,taskid)"
                    + " VALUES (?,?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setTimestamp(1,time);
            preparedStatement.setLong(2,tuplecount);
            preparedStatement.setInt(3,taskid);
            int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_spouttuplecount (time,tuplecount) values"+time+" "+tuplecount);
            JdbcPool.release(conn,preparedStatement,null);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
    /**
     * 插入数据到t_aresspoutlatency表中
     * @param timestamp
     * @param latencytime
     * @param taskid
     */
    public static void insertAresSpoutLatency(Timestamp timestamp, Long latencytime, int taskid) {
        try {
            conn=JdbcPool.getConnection();
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_aresspoutlatency(time,latencytime,taskid)"
                    + " VALUES (?,?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setTimestamp(1,timestamp);
            preparedStatement.setLong(2,latencytime);
            preparedStatement.setInt(3,taskid);
            int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_spouttuplecount (time,tuplecount) values"+time+" "+tuplecount);
            JdbcPool.release(conn,preparedStatement,null);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

}
