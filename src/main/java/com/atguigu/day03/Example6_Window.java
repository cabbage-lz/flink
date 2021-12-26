package com.atguigu.day03;

import com.atguigu.day02.ketang.Example01_Bean;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * description:TODO 全窗口聚合函数 计算PV
 * Created by thinkpad on 2021-09-21
 */
public class Example6_Window {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new Example01_Bean.ClickSource())
                .keyBy(r -> r.url)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new WindowResult())
                .print();

        env.execute();
    }

    //    public static class WindowResult extends ProcessWindowFunction<Example01_Bean.Event,String,String, TimeWindow>{
//        @Override
//        public void process(String key, Context context, Iterable<Example01_Bean.Event> elements, Collector<String> out) throws Exception {
////           窗口闭合时，窗口调用。 迭代器：迭代器中包含窗口中的所有数据  Iterable
//            long windowstart = context.window().getStart();
//            long windowend = context.window().getEnd();
////            计算迭代器中一共有多少元素
////            long count = elements.spliterator().getExactSizeIfKnown();
//            long count = 0L;
//            for (Example01_Bean.Event element : elements) {
//                count +=1L;
//            }
//            out.collect("url:"+key+"窗口："+new Timestamp(windowstart)+"~~~"+new Timestamp(windowend)+"的PV是："+count);
//
//        }
//    }
    public static class WindowResult extends ProcessWindowFunction<Example01_Bean.Event, UrlViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Example01_Bean.Event> elements, Collector<UrlViewCountPerWindow> out) throws Exception {
//        迭代器中是窗口中的所有元素
            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();
//        计算迭代器中元素的数量
            long count = elements.spliterator().getExactSizeIfKnown();
            out.collect(new UrlViewCountPerWindow(
                    s,
                    count,
                    windowStart,
                    windowEnd
            ));
        }
    }

    public static class UrlViewCountPerWindow {
        public String url;
        public Long count;
        public Long windowStart;
        public Long windowEnd;

        public UrlViewCountPerWindow() {
        }

        public UrlViewCountPerWindow(String url, Long count, Long windowStart, Long windowEnd) {
            this.url = url;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "url:" + url + "在窗口" + new Timestamp(windowStart) + "-" + new Timestamp(windowEnd) + "中的pv是" + count;
        }
    }
}
