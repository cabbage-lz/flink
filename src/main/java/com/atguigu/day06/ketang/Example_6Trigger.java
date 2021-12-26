package com.atguigu.day06.ketang;

import com.atguigu.day01.WordCount.Example1;
import com.atguigu.day02.ketang.Example01_Bean;
import com.atguigu.day03.Example6_Window;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * description:TODO chuf器
 * Created by thinkpad on 2021-09-24
 */
public class Example_6Trigger {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new Example01_Bean.ClickSource())
                .filter(r -> r.url.equals("./home"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Example01_Bean.Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Example01_Bean.Event>() {
                                    @Override
                                    public long extractTimestamp(Example01_Bean.Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })

                )
                .keyBy(r -> r.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(new MyTrigger())
                .process(new WindowResult())
                .print();

        env.execute();
    }

    //    TODO   触发器触发的后面的ProcessWindowFunction中process方法的执行  核心逻辑：每过1秒钟注册一个onEventTime 触发process方法的执行
    public static class MyTrigger extends Trigger<Example01_Bean.Event, TimeWindow> {
        // 每来一条数据，执行一次
        @Override
        public TriggerResult onElement(Example01_Bean.Event element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Boolean> isFirstEvent = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("is-first", Types.BOOLEAN));
            if (isFirstEvent.value() == null) {
                isFirstEvent.update(true);
                // 注册第一条事件的时间戳接下来的整数秒
                long nextIntTs = element.timestamp + 1000L - element.timestamp % 1000L;
                // 注册定时器其实是注册的onEventTime方法，例如注册一个2000毫秒的定时器，水位线到达2000毫秒时执行
                ctx.registerEventTimeTimer(nextIntTs);

            }
            return TriggerResult.CONTINUE;
        }

        // 机器时间到达`time`时执行，处理时间定时器
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {

            return TriggerResult.CONTINUE;
        }

        // 水位线到达`time`时执行，定时器
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            if (time < window.getEnd()) {
                ctx.registerEventTimeTimer(time + 1000L);
                return TriggerResult.FIRE;  // TODO 触发的是WindowResult的process方法的执行
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Boolean> isFirstEvent = ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("is-first", Types.BOOLEAN));
            isFirstEvent.clear();

        }
    }

    //                    TODO  I   O   K   W
    public static class WindowResult extends ProcessWindowFunction<Example01_Bean.Event, Example6_Window.UrlViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Example01_Bean.Event> elements, Collector<Example6_Window.UrlViewCountPerWindow> out) throws Exception {
            out.collect(new Example6_Window.UrlViewCountPerWindow(
                    s,
                    elements.spliterator().getExactSizeIfKnown(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }
}
