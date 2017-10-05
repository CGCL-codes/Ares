package com.basic.benchmark;

import org.junit.Test;

import java.util.Random;

/**
 * locate com.basic.benchmark
 * Created by 79875 on 2017/10/5.
 */
public class SentenceSpoutTest {

    private String[] sentences={
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "dont have acow man",
            "i dont think i like fleas",
            "i am very busy",
            "hello world i cant talk to you",
            "chinese is very nice i like it"
    };
    private Random random=new Random();

    private int index=0;

    @Test
    public void nextTuple() throws Exception {
        long startTime = System.nanoTime();
        int sentencesNum= random.nextInt(sentences.length);
        String sentence=sentences[sentencesNum];
        String[] split = sentence.split(" ");
        String word=split[random.nextInt(split.length)];

        index++;
        if(index>=sentences.length) index=0;
        long endTime = System.nanoTime();
        System.out.println(endTime-startTime);
    }

}
