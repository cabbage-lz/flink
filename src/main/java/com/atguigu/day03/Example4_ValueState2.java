package com.atguigu.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description:TODO 计算10秒钟内的平均值
 * Created by thinkpad on 2021-09-18
 */
public class Example4_ValueState2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env.execute();
    }

}
