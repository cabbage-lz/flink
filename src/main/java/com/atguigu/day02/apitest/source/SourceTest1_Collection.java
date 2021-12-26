package com.atguigu.day02.apitest.source;

import com.atguigu.day02.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * description:两个不同的流抢占一个slot资源，谁抢上谁执行
 * Created by thinkpad on 2021-09-16
 */


public class SourceTest1_Collection {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        从集合读取数据
        DataStream<SensorReading> dataStream = env.fromCollection(
                Arrays.asList(new SensorReading("1", 12345L, 15.4),
                        new SensorReading("2", 123456L, 16.4),
                        new SensorReading("3", 123457L, 17.4))
        );

        DataStream<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);
        //打印输出
        dataStream.print("data");
        integerDataStreamSource.print("int");//.setParallelism(1);

//        执行
        env.execute();

    }
}
