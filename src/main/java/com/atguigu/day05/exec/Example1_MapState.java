package com.atguigu.day05.exec;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description:TODO 用mapstate模拟滚动窗口，使用KeyedProcessFunction模拟窗口
 * TODO 窗口的本质也是定时器操作，keyBy后怎么通过窗口进行的逻辑分区
 * Created by thinkpad on 2021-09-23
 */
public class Example1_MapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env.execute();
    }
}
