package com.atguigu.function;

import com.atguigu.day06.ketang.Example2;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * description:TODO 双流实时对账 // select * from A inner join B where A.id = B.id;
 * Created by thinkpad on 2021-09-27
 */
public class TwoStreamJoin_DuiZhang {
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
        appZhifuStream
                .keyBy(r -> r.orderId)    //TODO 1.两条流进行join
                .connect(weixinZhifuStream.keyBy(r -> r.orderId))
                .process(new CoProcessFunction<Event, Event, String>() {
                    //                    TODO 2.定义两个状态变量，保存两条流的历史数据
                    private ValueState<Event> WeiXinState;
                    private ValueState<Event> APPState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        WeiXinState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("weixin-state", Types.POJO(Event.class)));
                        APPState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("APP-state", Types.POJO(Event.class)));
                    }

                    @Override    //TODO 3.处理微信支付事件
                    public void processElement1(Event value, Context ctx, Collector<String> out) throws Exception {
                        if (APPState.value() != null) {
                            out.collect("订单" + value.orderId + "对帐成功，app支付先到达，微信支付后到达");
                            APPState.clear();
                        } else {
                            WeiXinState.update(value);   //将value更新进状态变量
                            ctx.timerService().registerEventTimeTimer(value.timestamp + 5000L);
                        }
                    }

                    @Override     //TODO 4.处理APP支付事件
                    public void processElement2(Event value, Context ctx, Collector<String> out) throws Exception {
                        if (WeiXinState.value() != null) {
                            out.collect("订单" + value.orderId + "对账成功，微信支付先到达，app支付后到达");
                            WeiXinState.clear();
                        } else {
                            APPState.update(value); //将value更新进状态变量
                            ctx.timerService().registerEventTimeTimer(value.timestamp + 5000L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        if (APPState.value() != null) {

//                            out.collect("对账失败，微信支付未到");
                            out.collect("订单" + APPState.value().orderId + "对账失败，对应的微信支付事件未到");
                            APPState.clear();
                        }
                        if (WeiXinState.value() != null) {
//                            out.collect("对账失败，APP支付未到");
                            out.collect("订单" + WeiXinState.value().orderId + "对账失败，对应的app支付事件未到");
                            WeiXinState.clear();
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
