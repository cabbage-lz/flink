package com.atguigu.day02.apitest.source;

import com.atguigu.day02.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * description:自定义数据源
 * Created by thinkpad on 2021-09-16
 */
public class SourceTest3_UDF {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//       //从文件读取数据
        DataStreamSource<SensorReading> dataStream = env.addSource(new MySensorSource());
//        打印输出
        dataStream.print();
        env.execute();
    }

    //    TODO  自定义source
    public static class MySensorSource implements SourceFunction<SensorReading> {
        //定义标志位    ctx上下文
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
//        定义随机数发生器    nextGaussian高斯随机数  正态分布
//        设置10个传感器初始温度


            Random random = new Random();
            HashMap<String, Double> sensorTemMap = new HashMap<>();

            for (int i = 0; i < 10; i++) {
                sensorTemMap.put("sensor" + (i + 1), 60 + random.nextGaussian() * 20);
            }

            while (running) {
                for (String sensorId : sensorTemMap.keySet()) {
                    //在当前温度基础上随机波动
                    double newtemp = sensorTemMap.get(sensorId) + random.nextGaussian();
                    sensorTemMap.put(sensorId, newtemp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newtemp));
                }
//            控制输出频率
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
