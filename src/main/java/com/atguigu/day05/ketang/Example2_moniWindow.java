package com.atguigu.day05.ketang;

import com.atguigu.day02.ketang.Example01_Bean;
import com.atguigu.day03.Example6_Window;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * description:TODO 使用KeyedProcessFunction模拟窗口
 * Created by thinkpad on 2021-09-23
 */
public class Example2_moniWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new Example01_Bean.ClickSource())
                .assignTimestampsAndWatermarks(   //TODO  分配水位线和抽取时间戳
                        WatermarkStrategy.<Example01_Bean.Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Example01_Bean.Event>() {
                                    @Override
                                    public long extractTimestamp(Example01_Bean.Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .keyBy(r -> r.url)   //TODO 分流
                .process(new Window(5000L))  //TODO  5000毫秒的滚动窗口
                .print();

        env.execute();
    }

    //    KeyedProcessFunction模拟窗口
    public static class Window extends KeyedProcessFunction<String, Example01_Bean.Event, Example6_Window.UrlViewCountPerWindow> {
        private Long windowSize;
        //        KEY:窗口开窗时间   VALUE:窗口中对应的url的浏览次数
        private MapState<Long, Long> mapState;

        public Window(Long windowSizze) {
            this.windowSize = windowSizze;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, Long>("window-start-pv-count", Types.LONG, Types.LONG)
            );
        }

        @Override
        public void processElement(Example01_Bean.Event value, Context ctx, Collector<Example6_Window.UrlViewCountPerWindow> out) throws Exception {
            //TODO 来一条数据计算一次窗口的开始时间、关闭时间
            // 窗口开始时间 = 时间戳 - 时间戳对窗口大小取模
            // 7890ms - 7890ms % 5000ms === 5000ms
            long windowStart = value.timestamp - value.timestamp % windowSize;
            long windowEnd = windowStart + windowSize - 1L;
            //在窗口内，将窗口的开始时间和窗口中对应url的浏览次数更新进mapState字典状态变量
            if (!mapState.contains(windowStart)) {
                // 如   果不包含，说明这个窗口的第一条数据到达了，所以pv次数设置为1
                mapState.put(windowStart, 1L);
            } else {
                mapState.put(windowStart, mapState.get(windowStart) + 1L);
            }
            //TODO 窗口关闭的默认逻辑,注册窗口关闭的定时器，定时器用来输出结果
            ctx.timerService().registerEventTimeTimer(windowEnd);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Example6_Window.UrlViewCountPerWindow> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            long windowEnd = timestamp;                 //窗口结束时间
            long windowStart = windowEnd + 1L - windowSize;//c窗口开始时间
            long count = mapState.get(windowStart);      //窗口中的浏览次数
            String url = ctx.getCurrentKey();   //ctx.getKey就是url
            out.collect(new Example6_Window.UrlViewCountPerWindow(url, count, windowStart, windowEnd));
            // 销毁窗口
            mapState.remove(windowStart);             // 删除：remove(key)删除窗口
        }
    }
}
