package com.atguigu.day02.ketang;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * description:
 * Created by thinkpad on 2021-09-17
 */
public class Example8_rescale {

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

    public static class CustomSource extends RichParallelSourceFunction<Integer> {
        @Override
        public void run(SourceContext ctx) throws Exception {
            for (int i = 0; i < 8; i++) {

                if ((i + 1) % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                    ctx.collect(i + 1);
                }

            }
        }

        @Override
        public void cancel() {

        }
    }
}
