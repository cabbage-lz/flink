package com.atguigu.day05.ketang;

import com.atguigu.day04.ketang.Example9_anli;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * description:
 * TODO keyBy（ItemId）--》开窗--》聚合（增量聚合，全窗口聚合）得到每个itemID在每个窗口内的浏览次数
 * TODO --》keyBy分流（窗口结束时间）--》.process（top n）
 * TODO 列表状态变量、定时器、事件时间
 * Created by thinkpad on 2021-09-23
 */
public class Example6_anli_youhua {
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
                .keyBy(r -> r.itemId)  //每个逻辑分区的itemId是一样的
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))  //滑动事件窗口
                .aggregate(new CountAgg(), new WindowResult())   //  ==》聚合成一条流，这条流上的特点为ItemViewCountPerWindow。（分流-》开窗-》聚合）
                .keyBy(r -> r.windowEnd)   //继续分布式，使用窗口结束时间进行分流
                .process(new TopN(3))
                .print();

        env.execute();
    }

    // TODO   KeyedProcessFunction统计的是每个窗口的数据
    public static class TopN extends KeyedProcessFunction<Long, ItemViewCountPerWindow, String> {
        private int n;

        public TopN(int n) {
            this.n = n;
        }

        private ListState<ItemViewCountPerWindow> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCountPerWindow>("list-state", Types.POJO(ItemViewCountPerWindow.class))
            );
        }

        @Override
        public void processElement(ItemViewCountPerWindow value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1L);  //定时器只会注册一次，一个key在某个时间戳上只能注册一个定时器，
            //注册定时器用来排序
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            ArrayList<ItemViewCountPerWindow> itemViewCountPerWindows = new ArrayList<>();
            for (ItemViewCountPerWindow i : listState.get()) {    //列表状态变量里面的元素拿出来放进arrayList，因为列表状态变量是序列化的数据，
                itemViewCountPerWindows.add(i);
            }
            listState.clear();

            itemViewCountPerWindows.sort(new Comparator<ItemViewCountPerWindow>() {
                @Override
                public int compare(ItemViewCountPerWindow t1, ItemViewCountPerWindow t2) {
                    return t2.count.intValue() - t1.count.intValue();
                }
            });

            // 输出
            StringBuilder result = new StringBuilder();
            result.append("================================================\n");
            String windowEnd = new Timestamp(timestamp - 1L).toString();
            result.append("窗口结束时间：" + windowEnd + "中的最热门的三个商品是：\n");
            for (int i = 0; i < n; i++) {
                ItemViewCountPerWindow curr = itemViewCountPerWindows.get(i);
                result.append("第" + (i + 1) + "名商品的id是：" + curr.itemId + "，浏览次数是：" + curr.count);
                result.append("\n");
            }
            result.append("================================================\n");
            out.collect(result.toString());
        }
    }

    // TODO 全窗口聚合函数ProcessWindowFunction进行包装     I O K W
    public static class WindowResult extends ProcessWindowFunction<Long, ItemViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<ItemViewCountPerWindow> out) throws Exception {
            out.collect(new ItemViewCountPerWindow(
                    s,
                    elements.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }

    // TODO <I,累加,O>       增量聚合函数
    public static class CountAgg implements AggregateFunction<Example9_anli.UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Example9_anli.UserBehavior value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    //TODO 每个商品在每个窗口中的浏览次数
    public static class ItemViewCountPerWindow {
        public String itemId;
        public Long count;
        public Long windowStart;
        public Long windowEnd;

        public ItemViewCountPerWindow() {
        }

        public ItemViewCountPerWindow(String itemId, Long count, Long windowStart, Long windowEnd) {
            this.itemId = itemId;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "ItemViewCountPerWindow{" +
                    "itemId='" + itemId + '\'' +
                    ", count=" + count +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    '}';
        }
    }
}
