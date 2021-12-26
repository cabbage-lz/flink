package com.atguigu.day05.exec;

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

/**
 * description:TODO  列表状态变量,使用方法类似hashmap，模拟窗口，窗口的本质其实定时器
 * Created by thinkpad on 2021-09-23
 */
public class Example2_MapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new Example01_Bean.ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Example01_Bean.Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Example01_Bean.Event>() {
                            @Override
                            public long extractTimestamp(Example01_Bean.Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                )
                .keyBy(r -> r.url)
                .process(new FakeWindow(5000L))
                .print();

        env.execute();
    }

    public static class FakeWindow extends KeyedProcessFunction<String, Example01_Bean.Event, Example6_Window.UrlViewCountPerWindow> {
        private long windowSize;   //传进来的窗口大小
        private MapState<Long, Long> mapState;    //K V

        public FakeWindow(long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, Long>("windowStart-pv-count", Types.LONG, Types.LONG)
            );

        }

        @Override
        public void processElement(Example01_Bean.Event value, Context ctx, Collector<Example6_Window.UrlViewCountPerWindow> out) throws Exception {
//            滚动窗口的窗口开始时间计算公式：
            long windowStart = value.timestamp - value.timestamp % windowSize;
            long windowEnd = windowStart + windowSize - 1;
            if (!mapState.contains(windowStart)) {
                mapState.put(windowStart, 1L);    //说明窗口的第一条数据到达
            } else {
                mapState.put(windowStart, mapState.get(windowStart) + 1L);
            }
            ctx.timerService().registerEventTimeTimer(windowEnd);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Example6_Window.UrlViewCountPerWindow> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            long windowEnd = timestamp;
            long windowStart = timestamp - windowSize + 1;
            long count = mapState.get(windowStart);
            String url = ctx.getCurrentKey();
            out.collect(new Example6_Window.UrlViewCountPerWindow(url, count, windowStart, windowEnd));
            mapState.remove(windowStart);
        }
    }
}
