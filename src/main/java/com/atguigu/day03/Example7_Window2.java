package com.atguigu.day03;

import com.atguigu.day02.ketang.Example01_Bean;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * description:TODO  增量聚合函数
 * Created by thinkpad on 2021-09-21
 */
public class Example7_Window2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new Example01_Bean.ClickSource())
                .keyBy(r -> r.url)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new CountAgg())
                .print();

        env.execute();
    }

    public static class CountAgg implements AggregateFunction<Example01_Bean.Event, Long, Long> {
        @Override   //创建一个空累加器
        public Long createAccumulator() {
            return 0L;
        }

        @Override //定义累加规则,更新累加器
        public Long add(Example01_Bean.Event value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override //窗口闭合时，返回一个累加器
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }
}
