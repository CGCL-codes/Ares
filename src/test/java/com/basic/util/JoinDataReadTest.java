package com.basic.util;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * locate com.basic.util
 * Created by 79875 on 2017/10/20.
 */
public class JoinDataReadTest {
    @Test
    public void testRead() throws IOException {
        BufferedReader bufferedReader=new BufferedReader(new FileReader("C://Users/79875/Desktop/orders.tbl"));
        String s = bufferedReader.readLine();
        System.out.println(s);
        for(String tmp:s.split("\\|")){
            System.out.println(tmp);
        }
    }
}
