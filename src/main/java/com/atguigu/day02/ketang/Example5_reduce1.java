package com.atguigu.day02.ketang;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * description:
 * Created by thinkpad on 2021-09-17
 */
public class Example5_reduce1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO　求平均值
//        env
//                .addSource(new Example5_reduce.IntegerSource())
//                .map(r->Tuple2.of(r,1))
//                .returns(Types.TUPLE(Types.INT,Types.INT))
//                .keyBy(r -> true)
//                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
//                    @Override
//                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
//                        return Tuple2.of(value1.f0+value2.f0,value1.f1+value2.f1);
//                    }
//                })
//                .map(l->l.f0/l.f1)
//                .print();
        env
                .addSource(new Example5_reduce.IntegerSource())
                .map(r -> Tuple2.of(r, 1))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy(r -> "true")
                .reduce((r1, r2) -> Tuple2.of(r1.f0 + r2.f0, r1.f1 + r2.f1))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .print();

        env.execute();
    }


}
