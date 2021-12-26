package com.atguigu.day08.ketang;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * description:TODO 订单超时检测
 * 1、定义模板
 * 2、分流orderId匹配模板       CEP.pattern  .flatselect(三个参数 参数1 侧输出标签，参数2 处理超时事件的匿名类，参数3)
 * 3、从匹配流中将匹配到的事件取出来
 * 4、获取侧输出流 result.getSideOutput
 * Created by thinkpad on 2021-09-27
 */
public class Example4_OrderChaoShiJianCe {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        Event e1 = new Event("order-1", "create", 1000L);
                        ctx.collectWithTimestamp(e1, e1.ts);
                        Thread.sleep(1000L);
                        Event e2 = new Event("order-2", "create", 2000L);
                        ctx.collectWithTimestamp(e2, e2.ts);
                        Thread.sleep(1000L);
                        Event e3 = new Event("order-1", "pay", 4000L);
                        ctx.collectWithTimestamp(e3, e3.ts);
                        Thread.sleep(1000L);
                        ctx.emitWatermark(new Watermark(7000L));
                        Thread.sleep(1000L);
                        Event e4 = new Event("order-2", "pay", 8000L);
                        ctx.collectWithTimestamp(e4, e4.ts);
                        Thread.sleep(1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        //TODO 1.定义模板
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("create")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.type.equals("create");
                    }
                })
                .next("pay")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.type.equals("pay");
                    }
                })
                .within(Time.seconds(5));  //两个事件必须在5秒内完成
        //TODO 2. orderId匹配模板
        PatternStream<Event> patternStream = CEP.pattern(stream.keyBy(r -> r.orderId), pattern);

        SingleOutputStreamOperator<String> result = patternStream
                .flatSelect(    //TODO  flatSelect匹配5秒内的两个事件、只有create没有pay的超时操作
//                            TODO 参数1 侧输出标签，参数2 处理超时事件的匿名类，参数3
                        // 超时信息发送到这个侧输出流中去
                        new OutputTag<String>("timeout-order") {
                        },
                        new PatternFlatTimeoutFunction<Event, String>() {
                            @Override
                            public void timeout(Map<String, List<Event>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                                Event create = pattern.get("create").get(0);
                                // 发送到第一个参数对应的侧输出流
                                out.collect("订单" + create.orderId + "超时未支付");
                            }
                        },
                        new PatternFlatSelectFunction<Event, String>() {
                            @Override
                            public void flatSelect(Map<String, List<Event>> pattern, Collector<String> out) throws Exception {
                                Event pay = pattern.get("pay").get(0);    //TODO 将pay取出来 ，往下游发送
                                out.collect("订单" + pay.orderId + "支付成功");
                            }
                        }
                );

        result.getSideOutput(new OutputTag<String>("timeout-order") {
        }).print();

        result.print();

        env.execute();
    }

    public static class Event {
        public String orderId;
        public String type;
        public Long ts;

        public Event() {
        }

        public Event(String orderId, String type, Long ts) {
            this.orderId = orderId;
            this.type = type;
            this.ts = ts;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "orderId='" + orderId + '\'' +
                    ", type='" + type + '\'' +
                    ", ts=" + ts +
                    '}';
        }
    }

}
