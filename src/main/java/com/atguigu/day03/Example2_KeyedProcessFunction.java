package com.atguigu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * description:TODO // KeyedProcessFunction
 * Created by thinkpad on 2021-09-18
 */
public class Example2_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]));
                    }
                })
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Object>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Object> out) throws Exception {
                        out.collect("到达当前数据的key为：" + ctx.getCurrentKey());
                        out.collect("到达当前数据的时间为" + new Timestamp(ctx.timerService().currentProcessingTime()));
//                        TODO 注册定时器
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 20 * 1000L);
                        out.collect("注册一个定时器时间为：" + new Timestamp(ctx.timerService().currentProcessingTime() + 20 * 1000L));

                    }

                    //TODO 当时钟进行到定时器时间参数后处罚执行
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("定时器触发了，执行时间为" + new Timestamp(timestamp));
                    }
                })
                .print();

        env.execute();
    }
}
