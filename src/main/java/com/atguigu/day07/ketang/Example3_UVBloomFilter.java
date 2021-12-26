package com.atguigu.day07.ketang;

import com.atguigu.day04.ketang.Example9_anli;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * description:TODO  布隆过滤器
 * Created by thinkpad on 2021-09-25
 */
public class Example3_UVBloomFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .readTextFile("F:\\0428bigdate\\01java\\bigdatapro\\flink\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, Example9_anli.UserBehavior>() {
                    @Override
                    public Example9_anli.UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new Example9_anli.UserBehavior(
                                arr[0],
                                arr[1],
                                arr[2],
                                arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Example9_anli.UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Example9_anli.UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(Example9_anli.UserBehavior element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .keyBy(r -> 1)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new Example2_UVCount.CountAgg(), new Example2_UVCount.WindowResult())
                .print();

        env.execute();
    }

    //    TODO IOKW
    public static class WindowResult extends ProcessWindowFunction<Long, String, Integer, TimeWindow> {
        @Override
        public void process(Integer integer, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            out.collect("窗口" + new Timestamp(context.window().getStart()).toString() + "~~~" + new Timestamp(context.window().getEnd()).toString() +
                    "的UV是：" + elements.iterator().next());
        }
    }
    //TODO I ACC O

    public static class CountAgg implements AggregateFunction<Example9_anli.UserBehavior, Tuple2<Long, BloomFilter<Long>>, Long> {
        @Override
        public Tuple2<Long, BloomFilter<Long>> createAccumulator() {
            return Tuple2.of(0L, BloomFilter.create(Funnels.longFunnel(), 100000, 0.01));   //要过滤的类型Funnels.longFunnel()，去重后数据，误判率
        }

        @Override
        public Tuple2<Long, BloomFilter<Long>> add(Example9_anli.UserBehavior value, Tuple2<Long, BloomFilter<Long>> accumulator) {
            if (!accumulator.f1.mightContain(Long.parseLong(value.userId))) {    //表示用户一定没来过，将用户添加进去，
                accumulator.f1.put(Long.parseLong(value.userId));  // hash函数计算后置1
                accumulator.f0 += 1L;
            }
            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<Long, BloomFilter<Long>> accumulator) {
            return accumulator.f0;
        }

        @Override
        public Tuple2<Long, BloomFilter<Long>> merge(Tuple2<Long, BloomFilter<Long>> a, Tuple2<Long, BloomFilter<Long>> b) {
            return null;
        }
    }
}