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
 * description:TODO  FLinkCEP  连续登录三次失败
 * 1)
 * Created by thinkpad on 2021-09-27
 */
public class Example1_FlinkCEP {
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
                .<Event>begin("first")  //第一个事件满足的条件
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.type.equals("fail");
                    }
                })
                .next("second")    //连续的，严格紧邻
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.type.equals("fail");
                    }
                })
                .next("third")     //连续的，严格紧邻
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.type.equals("fail");
                    }
                });

//        TODO 2、在流上匹配模板    CEP.pattern第一个参数：流 第二个参数 模板
        PatternStream<Event> patternStream = CEP.pattern(stream.keyBy(r -> r.userId), pattern);
//        TODO 3.从匹配流中将匹配到的事件取出来    I  O
        patternStream
                .select(new PatternSelectFunction<Event, String>() {
                    @Override    //TODO  select:  map(first,list)
                    public String select(Map<String, List<Event>> pattern) throws Exception {
                        Event first = pattern.get("first").get(0);
                        Event second = pattern.get("second").get(0);
                        Event third = pattern.get("third").get(0);
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
