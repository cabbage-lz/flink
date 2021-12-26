package com.atguigu.day05.exec;

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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * description:TODO 实时热门商品优化,统计前三名的pv。
 * TODO 优化思路：尽量使用累计器和分布式，而不是keyby到同一条流
 * Created by thinkpad on 2021-09-26
 */
public class Example4_PVyouhua {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .readTextFile("F:\\0428bigdate\\01java\\bigdatapro\\flink\\src\\main\\resources\\UserBehavior.csv")
//                TODO 1.数据ETL
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
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Example9_anli.UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Example9_anli.UserBehavior>() {
                            @Override
                            public long extractTimestamp(Example9_anli.UserBehavior element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                )
//                TODO 2.数据分流开窗聚合
                .keyBy(r -> r.itemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new AggregateWindow(), new FullWindow())
                //TODO 3.按照窗口结束时间继续分流
                .keyBy(r -> r.windowEnd)
                .process(new Top(3))
                .print();


        env.execute();
    }

    //    TODO 2.1增量聚合，计算每个itemId的count   I  AC  O
    public static class AggregateWindow implements AggregateFunction<Example9_anli.UserBehavior, Long, Long> {
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


    //    TODO 2.2 全窗口聚合，包装统计聚合数据   IOKW
    public static class FullWindow extends ProcessWindowFunction<Long, ItemViewCountPerWindow1, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<ItemViewCountPerWindow1> out) throws Exception {
            out.collect(new ItemViewCountPerWindow1(
                    s,
                    elements.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }

    //TODO 每个商品在每个窗口中的浏览次数
    public static class ItemViewCountPerWindow1 {
        public String itemId;
        public Long count;
        public Long windowStart;
        public Long windowEnd;

        public ItemViewCountPerWindow1() {
        }

        public ItemViewCountPerWindow1(String itemId, Long count, Long windowStart, Long windowEnd) {
            this.itemId = itemId;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "ItemViewCountPerWindow1{" +
                    "itemId='" + itemId + '\'' +
                    ", count=" + count +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    '}';
        }
    }

    //    TODO WindowEnd分流后进行处理
    public static class Top extends KeyedProcessFunction<Long, ItemViewCountPerWindow1, String> {

        private int n;

        public Top(int n) {
            this.n = n;
        }

        private ListState<ItemViewCountPerWindow1> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCountPerWindow1>("list-state1", Types.POJO(ItemViewCountPerWindow1.class)));
        }

        @Override
        public void processElement(ItemViewCountPerWindow1 value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1L);   //针对一个key在某个时间戳上只能注册一个定时器

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            ArrayList<ItemViewCountPerWindow1> windowResult = new ArrayList<>();
            for (ItemViewCountPerWindow1 i : listState.get()) {
                windowResult.add(i);
            }
            listState.clear();
            windowResult.sort(new Comparator<ItemViewCountPerWindow1>() {
                @Override
                public int compare(ItemViewCountPerWindow1 o1, ItemViewCountPerWindow1 o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

            // 输出
            StringBuilder result = new StringBuilder();
            result.append("================================================\n");
            String windowEnd = new Timestamp(timestamp - 1L).toString();
            result.append("窗口结束时间：" + windowEnd + "中的最热门的三个商品是：\n");
            for (int i = 0; i < n; i++) {
                ItemViewCountPerWindow1 curr = windowResult.get(i);
                result.append("第" + (i + 1) + "名商品的id是：" + curr.itemId + "，浏览次数是：" + curr.count);
                result.append("\n");
            }
            result.append("================================================\n");
            out.collect(result.toString());
        }
    }
}
