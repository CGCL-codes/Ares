package com.basic.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;


/**
 * Created by 79875 on 2017/1/9.
 */
public class DataBaseUtil {

    private static Logger logger= LoggerFactory.getLogger(DataBaseUtil.class);
    public static final String jdbcUrl="jdbc:mysql://192.168.223.202:3306/aresbenchmark";
    public static final String username="root";
    public static final String passwrod="123456";

    static{
        getConnection();//获得数据库连接
    }

    //public static Logger logger= LoggerFactory.getLogger(DataBaseUtil.class);

    // 创建静态全局变量
    public static Connection conn;//创建用于连接数据库的Connection对象

    /* 获取数据库连接的函数*/
    public static Connection getConnection() {
        try {
            Class.forName("com.mysql.jdbc.Driver");// 加载Mysql数据驱动

            conn = DriverManager.getConnection(
                    jdbcUrl , username , passwrod);// 创建数据连接

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("数据库连接失败" + e.getMessage());
        }
        return conn; //返回所建立的数据库连接
    }

    //插入表测试代码
//    public static void insert(String word, Long value) {
//
//        Calendar Cld = Calendar.getInstance();
//        int millisecond = Cld.get(Calendar.MILLISECOND);
//        long systemTimeMillis=System.currentTimeMillis();
//        //这就是距离1970年1月1日0点0分0秒的毫秒数
//
//        try {
//            String sql = "INSERT INTO wordcount(word,count,millisecond,systemtime)"
//                    + " VALUES ('"+word+"','"+value+"','"+millisecond+"','"+systemTimeMillis+"')";  // 插入数据的sql语句
//
//            //REPLACE :如果插入的记录与表中原有的记录不重复，则执行INSERT操作，影响的记录数为1；
//            // 如果插入的记录与表中原有的记录重复，则先DELETE原有记录，再执行INSERT，影响的记录数为2。
//
//
//            st = (Statement) conn.createStatement();    // 创建用于执行静态sql语句的Statement对象
//
//            int count = st.executeUpdate(sql);  // 执行插入操作的sql语句，并返回插入数据的个数
//
//            System.out.println("向words表中插入 " + count + " 条数据"); //输出插入操作的处理结果
//
//        } catch (SQLException e) {
//            System.out.println("插入数据失败" + e.getMessage());
//        }
//    }

    /**
     * 插入数据到tupleCount表中
     * @param time
     * @param tuplecount
     */
    public static void insertTupleCount(Timestamp time,Long tuplecount){
        try {
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_tuplecount(time,tuplecount)"
                    + " VALUES (?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setTimestamp(1,time);
            preparedStatement.setLong(2,tuplecount);
            int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_tuplecount (time,tuplecount) values"+time+" "+tuplecount);
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
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_hdfsbytecount(time,bytecount)"
                    + " VALUES (?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setTimestamp(1,time);
            preparedStatement.setLong(2,bytecount);
            int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_hdfsbytecount (time,bytecount) values"+time+" "+bytecount);
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
            PreparedStatement preparedStatement;
            String sql = "INSERT INTO t_tuplelatency(time,tuplelatency)"
                    + " VALUES (?,?)";  // 插入数据的sql语句
            preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
            preparedStatement.setTimestamp(1,time);
            preparedStatement.setLong(2,latency);
            int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
            preparedStatement.close();
            //logger.info("insert into t_tuplelatency (time,tuplecount) values"+time+" "+latency);
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
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public static void insertDefaultSpoutCount(Timestamp time,Long tuplecount,int taskid){
        try {
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
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public static void closeconnection() throws SQLException {
        conn.close();
    }


}
