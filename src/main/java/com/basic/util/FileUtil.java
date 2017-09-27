package com.basic.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by 79875 on 2017/1/11.
 */
public class FileUtil {

    private static Logger logger= LoggerFactory.getLogger(FileUtil.class);

    /**
     * 创建文件
     * @param fileName
     * @return
     */
    public static boolean createFile(File fileName)throws Exception{
        boolean flag=false;
        try{
            if(!fileName.exists()){
                fileName.createNewFile();
                flag=true;
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 读TXT文件内容
     * @param fileName
     * @return
     */
    public static String readTxtFile(File fileName)throws Exception{
        String result=null;
        FileReader fileReader=null;
        BufferedReader bufferedReader=null;
        try{
            fileReader=new FileReader(fileName);
            bufferedReader=new BufferedReader(fileReader);
            try{
                String read=null;
                while((read=bufferedReader.readLine())!=null){
                    result=result+read+"\r\n";
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if(bufferedReader!=null){
                bufferedReader.close();
            }
            if(fileReader!=null){
                fileReader.close();
            }
        }
        System.out.println("读取出来的文件内容是："+"\r\n"+result);
        return result;
    }

    /**
     *  写入内容追加到指定文件中
     * @param fileName
     * @param content
     * @return
     * @throws Exception
     */
    public static boolean writeAppendTxtFile(File  fileName,String content)throws Exception{
        RandomAccessFile mm=null;
        boolean flag=false;
        BufferedOutputStream bufferedOutputStream=null;
        try {
            bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(fileName,true)) ;
            bufferedOutputStream.write(content.getBytes("UTF-8"));
            bufferedOutputStream.close();
//            mm=new RandomAccessFile(fileName,"rw");
//            mm.writeBytes(content);
            flag=true;
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }finally{
            if(mm!=null){
                mm.close();
            }
        }
        return flag;
    }

    /**
     * 写入内容到文件中
     * @param fileName
     * @param content
     * @return
     * @throws Exception
     */
    public static boolean writeTxtFile(File  fileName,String content)throws Exception{
        RandomAccessFile mm=null;
        boolean flag=false;
        BufferedOutputStream bufferedOutputStream=null;
        try {
            bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(fileName)) ;
            bufferedOutputStream.write(content.getBytes("UTF-8"));
            bufferedOutputStream.close();
//            mm=new RandomAccessFile(fileName,"rw");
//            mm.writeBytes(content);
            flag=true;
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }finally{
            if(mm!=null){
                mm.close();
            }
        }
        return flag;
    }



    public static void contentToTxt(String filePath, String content) {
        String str = new String(); //原有txt内容
        String s1 = new String();//内容更新
        try {
            File f = new File(filePath);
            if (f.exists()) {
                logger.info("文件存在");
            } else {
                logger.info("文件不存在");
                f.createNewFile();// 不存在则创建
            }
            BufferedReader input = new BufferedReader(new FileReader(f));

            while ((str = input.readLine()) != null) {
                s1 += str + "\n";
            }
            System.out.println(s1);
            input.close();
            s1 += content;

            BufferedWriter output = new BufferedWriter(new FileWriter(f));
            output.write(s1);
            output.close();
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
}
