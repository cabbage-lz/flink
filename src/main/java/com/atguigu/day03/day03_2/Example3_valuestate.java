package com.atguigu.day03.day03_2;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * description:TODO  值状态变量   计算平均值
 * Created by thinkpad on 2021-09-20
 */
public class Example3_valuestate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    private Random random = new Random();

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while (running) {
                            ctx.collect(random.nextInt(1000));
                            Thread.sleep(100L);
                        }

                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .keyBy(r -> 1)
                .process(new AVG())
                .print();

        env.execute();
    }

    //   TODO  KeyedProcessFunction  外部类
    public static class AVG extends KeyedProcessFunction<Integer, Integer, Double> {
        //        TODO  声明状态变量 保存 总和及多少条数据 元组
        private ValueState<Tuple2<Integer, Integer>> sumCount;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //TODO getState方法通过状态描述符（先new一个状态描述符）
            //先去状态后端（例如HDFS）寻找状态变量。
            //如果找不到，则初始化。如果找到了，则直接读取（processElement中执行）
            sumCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple2<Integer, Integer>>("sum_count", Types.TUPLE(Types.INT, Types.INT))
            );
        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<Double> out) throws Exception {
//TODO 如果状态变量里面为null，说明第一条数据到达
            if (sumCount.value() == null) {
                sumCount.update(Tuple2.of(value, 1));
            } else {
                Tuple2<Integer, Integer> temp = sumCount.value();
                sumCount.update(Tuple2.of(value + temp.f0, temp.f1 + 1));
            }
            out.collect((double) sumCount.value().f0 / sumCount.value().f1);
        }
    }
}
