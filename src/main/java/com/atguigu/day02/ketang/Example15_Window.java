package com.atguigu.day02.ketang;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * description:
 * Created by thinkpad on 2021-09-17
 */
public class Example15_Window {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new Example5_reduce.IntegerSource())
                .keyBy(r -> "1")
                .print();
//        .countWindow(12,12);
        env.execute();
    }
}
