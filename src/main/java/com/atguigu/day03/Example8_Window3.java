package com.atguigu.day03;

import com.atguigu.day02.ketang.Example01_Bean;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * description:TODO 增量与全量结合使用
 * Created by thinkpad on 2021-09-21
 */
public class Example8_Window3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new Example01_Bean.ClickSource())
                .keyBy(r -> r.url)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new Example7_Window2.CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    //    TODO  这里主要输入为：增量聚合函数的输出Long         I    O   K   W
    public static class WindowResult extends ProcessWindowFunction<Long, Example6_Window.UrlViewCountPerWindow, String, TimeWindow> {
//        TODO 注意：迭代器中只有一个元素，全量函数输出的结果


        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<Example6_Window.UrlViewCountPerWindow> out) throws Exception {
            //        迭代器中是窗口中的所有元素
            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();
//        获取迭代器中的值     TODO  "url:"+url+"在窗口"+new Timestamp(windowStart)+"-"+new Timestamp(windowEnd)+"中的pv是"+count
            long count = elements.iterator().next();
            out.collect(new Example6_Window.UrlViewCountPerWindow(
                    s,
                    count,
                    windowStart,
                    windowEnd
            ));
        }
    }

    //TODO  I ac O
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
