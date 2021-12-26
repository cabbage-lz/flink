package com.atguigu.day06.ketang;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

/**
 * description:
 * Created by thinkpad on 2021-09-24
 */
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> appZhifuStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        Event e1 = new Event("order-1", "app-zhifu", 1000L);
                        ctx.collectWithTimestamp(e1, e1.timestamp);    // 事件   事件时间戳
                        Thread.sleep(1000L);
                        ctx.emitWatermark(new Watermark(999L));    //水位线 999L
                        Thread.sleep(1000L);
                        Event e2 = new Event("order-2", "app-zhifu", 2000L);
                        ctx.collectWithTimestamp(e2, e2.timestamp);
                        Thread.sleep(1000L);
                        ctx.emitWatermark(new Watermark(7000L));    //8000之前触发定时事件
                        Thread.sleep(10L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        DataStreamSource<Event> weixinZhifuStream = env         //5s内将账对上
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        Event e1 = new Event("order-1", "weixin-zhifu", 4000L);
                        ctx.collectWithTimestamp(e1, e1.timestamp);     // 事件   事件时间戳4000L
                        Thread.sleep(1000L);
                        ctx.emitWatermark(new Watermark(3999L));   //水位线 3999L
                        Thread.sleep(1000L);
                        ctx.emitWatermark(new Watermark(7000L));
                        Thread.sleep(1000L);
                        Event e2 = new Event("order-2", "weixin-zhifu", 8000L);
                        ctx.collectWithTimestamp(e2, e2.timestamp);
                        Thread.sleep(1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        appZhifuStream.keyBy(r -> r.orderId)
                .connect(weixinZhifuStream.keyBy(r -> r.orderId))
                .process(new CoProcessFunction<Event, Event, String>() {
                    private ValueState<Event> appZhifuState;
                    private ValueState<Event> weixinState;

                    // TODO  两条流谁先到达，就把谁保存到状态变量中
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        appZhifuState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("app-zhifu", Types.POJO(Event.class)));
                        weixinState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("weixin-zhifu", Types.POJO(Event.class)));
                    }

                    @Override     //processElement1   APP支付事件
                    public void processElement1(Event value, Context ctx, Collector<String> out) throws Exception {
                        if (weixinState.value() != null) {    //说明微信支付先到 ，而APP支付也到了，说明对账成功
                            out.collect("订单" + value.orderId + "对账成功，微信支付先到达，app支付后到达");
                            weixinState.clear();
                        } else {
                            appZhifuState.update(value); //说明微信支付为空，APP支付先到，保存到状态变量，等待微信支付5秒
//                            TODO 哪个事件先到达就注册一个5秒钟的定时器，如果5秒内微信支付事件来了就对账成功，否则定时器就触发执行，对账失败
                            ctx.timerService().registerEventTimeTimer(value.timestamp + 5000L);
                        }
                    }

                    @Override      //processElement2   微信支付事件
                    public void processElement2(Event value, Context ctx, Collector<String> out) throws Exception {
                        if (appZhifuState.value() != null) {  //说明APP支付先到 ，而微信支付也到了，说明对账成功
                            out.collect("订单" + value.orderId + "对帐成功，app支付先到达，微信支付后到达");
                            appZhifuState.clear();
                        } else {
                            weixinState.update(value);  //说明APP支付为空，微信支付先到，保存到状态变量，等待APP支付5秒
                            //TODO 哪个事件先到达就注册一个5秒钟的定时器
                            ctx.timerService().registerEventTimeTimer(value.timestamp + 5000L);   //如果5s内对账成功了，状态变量就清除了
                        }
                    }

                    @Override       //TODO 5s的定时器触发了，
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        if (appZhifuState.value() != null) {   //说明5秒后，对应的微信支付事件没来
                            out.collect("订单" + appZhifuState.value().orderId + "对账失败，对应的微信支付事件未到");
                            appZhifuState.clear();
                        }
                        if (weixinState.value() != null) {    //说明5秒后，对应的APP支付事件没来
                            out.collect("订单" + weixinState.value().orderId + "对账失败，对应的app支付事件未到");
                            weixinState.clear();
                        }
                    }
                })
                .print();

        env.execute();
    }

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
