package com.atguigu.function;

import com.atguigu.day08.ketang.Example4_OrderChaoShiJianCe;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * description:
 * Created by thinkpad on 2021-09-30
 */
public class CEP_OrderJianCe {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
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

        env.execute();
    }

    //TODO 样例类
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
