package com.atguigu.day03;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.akka.org.jboss.netty.channel.ExceptionEvent;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * description:
 * Created by thinkpad on 2021-09-18
 */
public class Example5_SensorValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new SensorSource())
                .keyBy(r -> r.sensorId)
                .process(new TempIncreaseAlert())
                .print();


        env.execute();
    }

    public static class TempIncreaseAlert extends KeyedProcessFunction<String, SensorReading, String> {

        private ValueState<Double> lastTemp;
        private ValueState<Long> timerTs;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Types.DOUBLE));
            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Types.LONG));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
        }


        // TODO 1,2,3,4,5,6,7,6
        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double prevTemp = null;
            if (lastTemp.value() != null) {
                prevTemp = lastTemp.value();
            }
            lastTemp.update(value.temperature);
            Long ts = null;
            if (timerTs.value() != null) {
                ts = timerTs.value();
            }


        }
    }

    //    TODO 定义数据源类型
    public static class SensorReading {
        public String sensorId;
        public Double temperature;

        public SensorReading() {
        }

        public SensorReading(String sensorId, Double temperature) {
            this.sensorId = sensorId;
            this.temperature = temperature;
        }

        @Override
        public String toString() {
            return "SensorSource{" +
                    "sensorId='" + sensorId + '\'' +
                    ", temperature=" + temperature +
                    '}';
        }
    }

    //    TODO 定义数据源
    public static class SensorSource implements SourceFunction<SensorReading> {
        private boolean running = true;
        private Random random = new Random();
        private String[] sensorIds = {"sensor_1", "sensor_2", "sensor_3"};

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while (running) {
                for (int i = 0; i < 3; i++) {
                    ctx.collect(new SensorReading("sensor_" + (i + 1), random.nextGaussian()));
                }
                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
