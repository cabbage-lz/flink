package com.atguigu.day02.ketang;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description:
 * Created by thinkpad on 2021-09-17
 */
public class Example10_Rich {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromElements(1, 2, 3, 4, 5)
                .map(new RichMapFunction<Integer, String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("子任务索引为：" + getRuntimeContext().getIndexOfThisSubtask() + "生命周期开始");
                    }

                    @Override
                    public String map(Integer value) throws Exception {
                        return value + "在子任务" + getRuntimeContext().getIndexOfThisSubtask() + "中处理，结果为：" + value * value;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("子任务索引为：" + getRuntimeContext().getIndexOfThisSubtask() + "生命周期结束");
                    }
                })
                .setParallelism(2)
                .print();

        env.execute();
    }
}
