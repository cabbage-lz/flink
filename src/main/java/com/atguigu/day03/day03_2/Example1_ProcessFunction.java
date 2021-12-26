package com.atguigu.day03.day03_2;

import com.atguigu.day02.ketang.Example01_Bean;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * description:TODO  ProcessFunction
 * Created by thinkpad on 2021-09-19
 */
public class Example1_ProcessFunction {
    public static void main(String[] args) throws Exception {
//        环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new Example01_Bean.ClickSource())
                .process(new ProcessFunction<Example01_Bean.Event, String>() {

                    @Override
                    public void processElement(Example01_Bean.Event value, Context ctx, Collector<String> out) throws Exception {
//                        out.collect("数据到达process算子的时间为："+new Timestamp(ctx.timerService().currentProcessingTime()));
                        out.collect("数据到达process算子的时间为：" + new Timestamp(ctx.timerService().currentProcessingTime()));
                        out.collect("点击事件的用户:" + value.user);
                        out.collect("点击的url:" + value.url);
                    }
                })
                .print();

        env.execute();
    }
}
