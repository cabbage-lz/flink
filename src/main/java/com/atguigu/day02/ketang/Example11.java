package com.atguigu.day02.ketang;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * description:TODO 并行任务富函数
 * Created by thinkpad on 2021-09-17
 */
public class Example11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new CustomSource())
                .setParallelism(2)
                .rescale()
                .print()
                .setParallelism(4);
        env.execute();
    }

    //自定义并行任务数据源
    public static class CustomSource extends RichParallelSourceFunction<Integer> {

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            for (int i = 0; i < 8; i++) {
                if ((i + 1) % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                    ctx.collect(i);
                }
            }
        }

        @Override
        public void cancel() {

        }
    }

}
