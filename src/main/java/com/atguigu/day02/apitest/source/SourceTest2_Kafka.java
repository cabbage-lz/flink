package com.atguigu.day02.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.zookeeper.FlinkZooKeeperQuorumPeer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.stream.Stream;

/**
 * description:
 * Created by thinkpad on 2021-09-16
 */
public class SourceTest2_Kafka {

    public static void main(String[] args) throws Exception {
        //设置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        //读取数据源
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));

        dataStream.print();
        env.execute();

    }

}
