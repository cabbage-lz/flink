package com.atguigu.day02.ketang;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * description:
 * Created by thinkpad on 2021-09-17
 */
public class Example4_flatmap {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromElements("white", "black", "green")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        if (value.equals("white")) {
                            out.collect(value);
                        } else if (value.equals("black")) {
                            out.collect(value);
//                            out.collect(value);
                        }
                    }
                })
                .print();
        env
                .fromElements("white", "black", "green")
                .flatMap((String value, Collector<String> out) -> {
                    if (value.equals("white")) {
                        out.collect(value);
                    } else if (value.equals("black")) {
                        out.collect(value);
//                        out.collect(value);
                    }
                })
                .returns(Types.STRING)
                .print();


        env.execute();

    }

}
