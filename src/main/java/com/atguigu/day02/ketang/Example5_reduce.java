package com.atguigu.day02.ketang;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.common.metrics.stats.Max;

import java.util.Random;

/**
 * description:
 * Created by thinkpad on 2021-09-17
 */
public class Example5_reduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//TODO 求平均值
        env
                .addSource(new IntegerSource())
                .map(r -> Tuple2.of(r, 1))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy(r -> true)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0 + value2.f0, value1.f1 + value2.f1);
                    }
                })
                .map(r -> r.f0 / r.f1)
                .print("平均值：");
//        TODO 求最大值和最小值
        env
                .addSource(new IntegerSource())
                .map(r -> Tuple2.of(r, r))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy(r -> 1)
                .reduce((Tuple2<Integer, Integer> r1, Tuple2<Integer, Integer> r2) -> Tuple2.of(Math.max(r1.f0, r2.f0), Math.min(r1.f1, r2.f1)))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .print("最大值/最小值：");

//        TODO 使用sum累加
        env
                .addSource(new IntegerSource())
                .keyBy(r -> 1)
                .sum(0)
                .print("总和为：");


        env.execute();

    }

    public static class IntegerSource implements SourceFunction<Integer> {
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
    }

}
