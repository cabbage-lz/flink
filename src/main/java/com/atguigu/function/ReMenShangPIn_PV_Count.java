package com.atguigu.function;


import com.atguigu.day05.ketang.Example6_anli_youhua;
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
import java.util.stream.Stream;

/**
 * description:TODO  实时热门商品TOP(n)
 * Created by thinkpad on 2021-09-27
 */
public class ReMenShangPIn_PV_Count {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .readTextFile("F:\\0428bigdate\\01java\\bigdatapro\\flink\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(
                                arr[0],
                                arr[1],
                                arr[2],
                                arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                )
                .keyBy(r -> r.itemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new Aggregate(), new FullWindowCount())
                .keyBy(r -> r.windowEnd)
                .process(new keyedProcess(5))
                .print();

        env.execute();
    }

    // TODO 以窗口结束时间进行汇总
    public static class keyedProcess extends KeyedProcessFunction<Long, ItemViewCountPerWindow2, String> {
        private int n;

        public keyedProcess(int n) {
            this.n = n;
        }

        private ListState<ItemViewCountPerWindow2> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCountPerWindow2>("list_state", Types.POJO(ItemViewCountPerWindow2.class)));
        }

        @Override
        public void processElement(ItemViewCountPerWindow2 value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            ArrayList<ItemViewCountPerWindow2> itemViewCountPerWindows = new ArrayList<>();
            for (ItemViewCountPerWindow2 i : listState.get()) {
                itemViewCountPerWindows.add(i);
            }
            listState.clear();
            itemViewCountPerWindows.sort(new Comparator<ItemViewCountPerWindow2>() {
                @Override
                public int compare(ItemViewCountPerWindow2 o1, ItemViewCountPerWindow2 o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

            // 输出
            StringBuilder result = new StringBuilder();
            result.append("================================================\n");
            String windowEnd = new Timestamp(timestamp - 1L).toString();
            result.append("窗口结束时间：" + windowEnd + "中的最热门的三个商品是：\n");
            for (int i = 0; i < n; i++) {
                ItemViewCountPerWindow2 curr = itemViewCountPerWindows.get(i);
                result.append("第" + (i + 1) + "名商品的id是：" + curr.itemId + "，浏览次数是：" + curr.count);
                result.append("\n");
            }
            result.append("================================================\n");
            out.collect(result.toString());


        }
    }

    //    TODO 定义全窗口聚合函数 <I  O K W>
    public static class FullWindowCount extends ProcessWindowFunction<Long, ItemViewCountPerWindow2, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<ItemViewCountPerWindow2> out) throws Exception {
            out.collect(new ItemViewCountPerWindow2(
                    s,
                    elements.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }

    //    TODO 定义增量聚合函数 <I AC O>
    public static class Aggregate implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
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

    //    TODO  用户行为POJO类
    public static class UserBehavior {
        public String userId;
        public String itemId;
        public String categoryId;
        public String behavior;
        public Long timestamp;

        public UserBehavior() {
        }

        public UserBehavior(String userId, String itemId, String categoryId, String behavior, Long timestamp) {
            this.userId = userId;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.behavior = behavior;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "UserBehavior{" +
                    "userId='" + userId + '\'' +
                    ", itemId='" + itemId + '\'' +
                    ", categoryId='" + categoryId + '\'' +
                    ", behavior='" + behavior + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }

    //    TODO  商品统计POJO类
    public static class ItemViewCountPerWindow2 {
        public String itemId;
        public Long count;
        public Long windowStart;
        public Long windowEnd;

        public ItemViewCountPerWindow2() {
        }

        public ItemViewCountPerWindow2(String itemId, Long count, Long windowStart, Long windowEnd) {
            this.itemId = itemId;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "ItemViewCountPerWindow2{" +
                    "itemId='" + itemId + '\'' +
                    ", count=" + count +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    '}';
        }
    }
}
