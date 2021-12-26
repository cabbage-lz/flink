package com.atguigu.day03;

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
 * description:
 * Created by thinkpad on 2021-09-18
 */

/*TODO  getRuntimeContext() 方法提供了函数的 RuntimeContext 的一些信息，例如函数
 * 执行的并行度，当前子任务的索引，当前子任务的名字。同时还它还包含了访问分区状
 * 态的方法。*/
public class Example3_ValueState {
    //TODO 计算平均值
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
                            Thread.sleep(1000L);
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

    public static class AVG extends KeyedProcessFunction<Integer, Integer, Double> {

        //        声明状态变量
//        将累加器暴露出来，自定义累加器的类型
        private ValueState<Tuple2<Integer, Integer>> sumCount;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
//            TODO 获取值状态变量
//            TODO 通过状态描述符去状态后端找状态变量
            sumCount = getRuntimeContext().getState(new ValueStateDescriptor<Tuple2<Integer, Integer>>(
                    "sum_count", Types.TUPLE(Types.INT, Types.INT)
            ));

        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<Double> out) throws Exception {
//TODO 如果值状态变量为null，第一条数据到达
            if (sumCount.value() == null) {
                sumCount.update(Tuple2.of(value, 1));
            } else {
//                定义临时变量
                Tuple2<Integer, Integer> temp = sumCount.value();
                sumCount.update(Tuple2.of(temp.f0 + value, temp.f1 + 1));
            }
            out.collect((double) sumCount.value().f0 / sumCount.value().f1);
        }
    }

}
