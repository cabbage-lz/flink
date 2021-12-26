package com.atguigu.day06.ketang;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * description:TODO 基于间隔（intervalJoin）的join    时间段的设置
 * Created by thinkpad on 2021-09-24
 */
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        TODO  下单时间与物流时间join
        SingleOutputStreamOperator<Event> buyStream = env
                .fromElements(new Event("order-1", "buy", 1000L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.timestamp;
                                            }
                                        }
                                )
                );
        SingleOutputStreamOperator<Event> shipStream = env
                .fromElements(
                        new Event("order-1", "ship", 10000 * 1000L),
                        new Event("order-1", "ship", 30 * 3600 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.timestamp;
                                            }
                                        }
                                )
                );
        buyStream
                .keyBy(r -> r.orderId)
                .intervalJoin(shipStream.keyBy(r -> r.orderId))
                .between(Time.days(0), Time.days(1))
                .process(new ProcessJoinFunction<Event, Event, Object>() {
                    @Override
                    public void processElement(Event left, Event right, Context ctx, Collector<Object> out) throws Exception {
                        out.collect(left + "=>" + right);
                    }
                })
                .print();
        env.execute();
    }

    //    TODO 事件
    public static class Event {
        public String orderId;
        public String type;
        public Long timestamp;

        public Event(String orderId, String type, Long timestamp) {
            this.orderId = orderId;
            this.type = type;
            this.timestamp = timestamp;
        }

        public Event() {
        }

        @Override
        public String toString() {
            return "Event{" +
                    "orderId='" + orderId + '\'' +
                    ", type='" + type + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
