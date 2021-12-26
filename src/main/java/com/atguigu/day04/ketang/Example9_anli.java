package com.atguigu.day04.ketang;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Set;

/**
 * description:TODO  实时热门商品
 * Created by thinkpad on 2021-09-22
 */
public class Example9_anli {
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
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                )
                .windowAll(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .process(new WindowResult())
                .print();
//                TODO 统计的为每个窗口中的实时热门商品或者每隔5分钟的最热门商品（也就是PV最多）

        env.execute();
    }

    public static class WindowResult extends ProcessAllWindowFunction<UserBehavior, String, TimeWindow> {
        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<String> out) throws Exception {
//            TODO  使用hashmap处理   key：itemId; value:pv-count
            HashMap<String, Long> itemCount = new HashMap<>();
//            TODO  value做降序排列，输出前三个
            for (UserBehavior e : elements) {
                if (!itemCount.containsKey(e.itemId)) {
                    itemCount.put(e.itemId, 1L);
                } else {
                    itemCount.put(e.itemId, itemCount.get(e.itemId) + 1L);
                }
            }
//            TODO hash表中降序排列，  将kv封装成一个元组
            ArrayList<Tuple2<String, Long>> arrayList = new ArrayList<>();

            for (String key : itemCount.keySet()) {
                arrayList.add(Tuple2.of(key, itemCount.get(key)));
            }
            arrayList.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> t1, Tuple2<String, Long> t2) {
                    return t1.f1.intValue() - t2.f1.intValue();
                }
            });
//            TODO  输出
            StringBuilder result = new StringBuilder();
            result.append("=========================\n");
            String windowStart = new Timestamp(context.window().getStart()).toString();
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            result.append("窗口：" + windowStart + "-" + windowEnd + "中的最热门的三个商品是:\n");
            for (int i = 0; i < 3; i++) {
                Tuple2<String, Long> curr = arrayList.get(i);
                result.append("第" + (i + 1) + "名商品的id是：" + curr.f0 + ",浏览次数是：" + curr.f1);
                result.append("\n");
            }
            result.append("============================\n");
            out.collect(result.toString());
        }
    }

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
}
