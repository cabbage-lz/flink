package com.atguigu.day02.ketang;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * description:
 * Created by thinkpad on 2021-09-17
 */
public class Example14_ToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env
//                .addSource(new Example01_Bean.ClickSource())
//                .addSink(new FlinkKafkaProducer<Example01_Bean.Event>("hadoop102:9092","sinkTest",new Example01_Bean.Event()));

        env.execute();
    }
}
