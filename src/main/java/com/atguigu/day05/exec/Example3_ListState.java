package com.atguigu.day05.exec;

import com.atguigu.day02.ketang.Example5_reduce;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * description:列表状态变量  求流上的平均值
 * Created by thinkpad on 2021-09-26
 */
public class Example3_ListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new Example5_reduce.IntegerSource())
                .keyBy(r -> 1)
                .process(new KeyedProcessFunction<Integer, Integer, Double>() {
                    private ListState<Integer> listState;
                    private ValueState<Long> timerTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listState = getRuntimeContext().getListState(
                                new ListStateDescriptor<Integer>("history-data", Types.INT)
                        );
                        timerTs = getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("timer", Types.LONG)
                        );
                    }

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Double> out) throws Exception {
                        listState.add(value);
                        Long tenSecondLater;
                        if (timerTs.value() == null) {
                            tenSecondLater = ctx.timerService().currentProcessingTime() + 10 * 1000L;
                            ctx.timerService().registerProcessingTimeTimer(tenSecondLater);
                            timerTs.update(tenSecondLater);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Double> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        long sum = 0L;
                        long count = 0L;
                        for (Integer i : listState.get()) {
                            sum += i;
                            count += 1;
                        }
                        out.collect((double) sum / count);
                        timerTs.clear();
                    }
                })
                .print();

        env.execute();
    }
}
