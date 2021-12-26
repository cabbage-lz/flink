package com.atguigu.day04.ketang;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description:TODO 多条流的合并
 * Created by thinkpad on 2021-09-22
 */
public class Example5_UNION {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> stream2 = env.fromElements(4, 5, 6);
        DataStreamSource<Integer> stream3 = env.fromElements(7, 8, 9);
        stream1.union(stream2).union(stream3).print();

        env.execute();
    }
}
