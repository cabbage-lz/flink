package com.atguigu.day06.ketang;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * description:TODO  等值内连接
 * Created by thinkpad on 2021-09-27
 */
public class Example1 {
    // select * from A inner join B where A.id = B.id;
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple2<String, Integer>> stream1 = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("a", 2),
                        Tuple2.of("b", 1),
                        Tuple2.of("b", 2)
                );

        DataStreamSource<Tuple2<String, Integer>> stream2 = env
                .fromElements(
                        Tuple2.of("a", 3),
                        Tuple2.of("a", 4),
                        Tuple2.of("b", 3),
                        Tuple2.of("b", 4)
                );

        stream1.keyBy(r -> r.f0)
                .connect(stream2.keyBy(r -> r.f0))
                .process(new CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Object>() {
                    //                    TODO 用来保存每一条流的历史数据
                    private ListState<Tuple2<String, Integer>> listState1;
                    private ListState<Tuple2<String, Integer>> listState2;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listState1 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Integer>>("list1", Types.TUPLE(Types.STRING, Types.INT)));
                        listState2 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Integer>>("list2", Types.TUPLE(Types.STRING, Types.INT)));
                    }

                    @Override     //TODO CoProcessFunction处理的同一个key的数据，两条流把key相同的数据发到同一个逻辑分区，去做合流，
//TODO  来自第一条流的数据合流时调用processElement1，来自第二条流的数据合流时调用processElement2，
                    public void processElement1(Tuple2<String, Integer> left, Context ctx, Collector<Object> out) throws Exception {
                        listState1.add(left);
                        for (Tuple2<String, Integer> right : listState2.get()) {
                            out.collect(left + "=>" + right);
                        }
                    }

                    @Override
                    public void processElement2(Tuple2<String, Integer> right, Context ctx, Collector<Object> out) throws Exception {
                        listState2.add(right);
                        for (Tuple2<String, Integer> left : listState1.get()) {
                            out.collect(left + "=>" + right);
                        }

                    }
                })
                .print();


        env.execute();
    }
}
