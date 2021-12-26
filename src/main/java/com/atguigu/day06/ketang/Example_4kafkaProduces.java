package com.atguigu.day06.ketang;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * description:TODO  Sink to kafka
 * Created by thinkpad on 2021-09-24
 */
public class Example_4kafkaProduces {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> stream = env.readTextFile("F:\\0428bigdate\\01java\\bigdatapro\\flink\\src\\main\\resources\\UserBehavior.csv");

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");
        stream
                .addSink(
                        new FlinkKafkaProducer<String>(
                                "user-behavior",
                                new SimpleStringSchema(),  //
                                properties
                        )
                );
        env.execute();
    }
}
