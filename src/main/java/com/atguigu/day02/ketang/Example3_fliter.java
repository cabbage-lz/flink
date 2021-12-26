package com.atguigu.day02.ketang;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * description:filter
 * Created by thinkpad on 2021-09-17
 */
public class Example3_fliter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new Example01_Bean.ClickSource())
                .filter(new FilterFunction<Example01_Bean.Event>() {
                    @Override
                    public boolean filter(Example01_Bean.Event value) throws Exception {
//                        return value.user.equals("james");
                        return value.user.startsWith("j");
                    }
                })
                .print();
        env
                .addSource(new Example01_Bean.ClickSource())
                .filter(new MyFilter())
                .print();
        env
                .addSource(new Example01_Bean.ClickSource())
                .filter(r -> r.user.equals("james"))
                .print();
        env
                .addSource(new Example01_Bean.ClickSource())
                .flatMap(new FlatMapFunction<Example01_Bean.Event, Example01_Bean.Event>() {
                    @Override
                    public void flatMap(Example01_Bean.Event value, Collector<Example01_Bean.Event> out) throws Exception {
                        if (value.user.equals("james"))
                            out.collect(value);
                    }
                })
                .print();

        env.execute();
    }

    public static class MyFilter implements FilterFunction<Example01_Bean.Event> {
        @Override
        public boolean filter(Example01_Bean.Event value) throws Exception {
            return value.user.startsWith("t");
        }
    }
}
