package com.atguigu.day02.ketang;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description:
 * Created by thinkpad on 2021-09-17
 */
public class Example6_shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromElements(1, 2, 3, 4)
//                .shuffle()
                .rebalance()
                .print()
                .setParallelism(2);


        env.execute();
    }
}
