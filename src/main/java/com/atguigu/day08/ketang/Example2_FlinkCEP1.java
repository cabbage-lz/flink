package com.atguigu.day08.ketang;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * description:TODO 优化CEP
 * Created by thinkpad on 2021-09-27
 */
public class Example2_FlinkCEP1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env
                .fromElements(
                        new Event("user-1", "fail", 1000L),
                        new Event("user-1", "fail", 2000L),
                        new Event("user-2", "success", 3000L),
                        new Event("user-1", "fail", 4000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                );

//TODO  1、定义模板
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("loginfail")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.type.equals("fail");
                    }
                })
                .times(3)
                .consecutive();//连续的


//        TODO 2、在流上匹配模板
        PatternStream<Event> patternStream = CEP.pattern(stream.keyBy(r -> r.userId), pattern);
//        TODO 3.从匹配流中将匹配到的事件取出来
        patternStream
                .select(new PatternSelectFunction<Event, String>() {
                    @Override
                    public String select(Map<String, List<Event>> pattern) throws Exception {
                        Event first = pattern.get("loginfail").get(0);
                        Event second = pattern.get("loginfail").get(1);
                        Event third = pattern.get("loginfail").get(2);
                        String result = "用户" + first.userId + "连续三次登录失败，登录时间是：" + first.ts + ";" + second.ts + ";" + third.ts;
                        return result;
                    }
                })
                .print();
        env.execute();
    }

    //TODO 定义POJO类
    public static class Event {
        public String userId;
        public String type;
        public Long ts;

        public Event() {
        }

        public Event(String userId, String type, Long ts) {
            this.userId = userId;
            this.type = type;
            this.ts = ts;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "userId='" + userId + '\'' +
                    ", type='" + type + '\'' +
                    ", ts=" + ts +
                    '}';
        }
    }
}
