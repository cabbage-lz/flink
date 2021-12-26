package com.atguigu.day05.ketang;

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

import java.time.temporal.ValueRange;

/**
 * description:TODO  ListState状态变量     求流上数据的平均值
 * Created by thinkpad on 2021-09-23
 */
public class Example1_ListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new Example5_reduce.IntegerSource())
                .keyBy(r -> 1)
                .process(new KeyedProcessFunction<Integer, Integer, Double>() {
                    //TODO  声明liststate状态变量
                    private ListState<Integer> listState;
                    //TODO  设置定时器定时去输出
                    private ValueState<Long> timerTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listState = getRuntimeContext().getListState(
                                new ListStateDescriptor<Integer>("history-data", Types.INT
                                ));
                        timerTs = getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("timer", Types.LONG
                                ));

                    }

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Double> out) throws Exception {
                        //TODO 每来一条数据就将数据添加到列表状态变量中
                        listState.add(value);
                        if (timerTs.value() == null) {
                            Long tenSecondlater = ctx.timerService().currentProcessingTime() + 10 * 1000L;
                            ctx.timerService().registerProcessingTimeTimer(tenSecondlater);
                            timerTs.update(tenSecondlater);
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
