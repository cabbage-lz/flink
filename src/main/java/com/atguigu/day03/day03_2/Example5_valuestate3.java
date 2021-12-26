package com.atguigu.day03.day03_2;

import com.atguigu.day03.Example5_SensorValueState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * description:TODO 连续一秒钟温度上升检测
 * Created by thinkpad on 2021-09-21
 */
public class Example5_valuestate3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new SensorReading.SensorSource())
                .keyBy(r -> r.sensorId)
                .process(new TemIncreaseAlert())
                .print();
        env.execute();
    }

    public static class TemIncreaseAlert extends KeyedProcessFunction<String, SensorReading, String> {

        //     1.定义两个状态变量
        private ValueState<Double> lastTemp;    //最近一次温度
        private ValueState<Long> timerTs;   //保存定时器的时间戳


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
//            2. 状态变量赋值
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last_temp", Types.DOUBLE));
            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time", Types.LONG));

        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double prevTemp = null;   //临时存储最近一次的温度
            if (lastTemp.value() != null) {
                prevTemp = lastTemp.value();//从第二条数据来了后，才能将lastTemp的温度赋值给prevTemp
            }
            lastTemp.update(value.temperature);  //将当前温度更新进lastTemp

            Long ts = null;   // 存储定时器时间
            if (timerTs.value() != null) {
                ts = timerTs.value();  //当温度连续两次上升时，timerTs.value()才有值
            }

            if (prevTemp == null || prevTemp > value.temperature) {   //prevTemp == null说明lastTemp中没有数据，也就是说来的数据为第一条
                if (ts != null) {           //TODO  温度下降时删除并清空定时器
                    ctx.timerService().deleteProcessingTimeTimer(ts);
                    timerTs.clear();
                }

            } else if (prevTemp < value.temperature && ts == null) {   //TODO 温度上升，且报警定时器为空，说明没有定时器，此时注册定时器
                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L);
                timerTs.update(ctx.timerService().currentProcessingTime() + 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
//            4.输出      TODO 达到定时器的时间时，输出报警
            out.collect("传感器：" + ctx.getCurrentKey() + "连续1秒钟温度上升");

            timerTs.clear();

        }
    }

    //   TODO  传感器温度类
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
            return "SensorReading{" +
                    "sensorId='" + sensorId + '\'' +
                    ", temperature=" + temperature +
                    '}';
        }

        //   TODO     数据源类
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

            }
        }
    }
}
