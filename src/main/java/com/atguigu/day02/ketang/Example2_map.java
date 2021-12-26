package com.atguigu.day02.ketang;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * description:TODO map算子    取出user
 * Created by thinkpad on 2021-09-17
 */
public class Example2_map {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new Example01_Bean.ClickSource())
                .map(new MapFunction<Example01_Bean.Event, String>() {
                    @Override
                    public String map(Example01_Bean.Event value) throws Exception {
                        return value.user;
                    }
                });
        env
                .addSource(new Example01_Bean.ClickSource())
                .map(new MyMap())
                .print();
        env
                .addSource(new Example01_Bean.ClickSource())
                .map(r -> r.user)
                .print();


//        TODO 使用flatmap实现
        env
                .addSource(new Example01_Bean.ClickSource())
                .flatMap(new FlatMapFunction<Example01_Bean.Event, String>() {
                    @Override
                    public void flatMap(Example01_Bean.Event value, Collector<String> out) throws Exception {
                        out.collect(value.user);
                    }
                });
        env.execute();
    }

    public static class MyMap implements MapFunction<Example01_Bean.Event, String> {
        @Override
        public String map(Example01_Bean.Event value) throws Exception {
            return value.user;
        }
    }

}
