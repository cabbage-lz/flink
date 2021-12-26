package com.atguigu.day01.WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * description:
 * Created by thinkpad on 2021-09-15
 */
public class Example3 {

    public static void main(String[] args) throws Exception {
//        1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        2.读取数据源
        DataStreamSource<String> path = env.readTextFile("F:\\0428bigdate\\01java\\bigdatapro\\flink\\src\\main\\resources\\hello.txt");

//        3.map--》keyBy-->reduce
//        SingleOutputStreamOperator<Tuple2<String, Integer>> mappedStream = path.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                String[] arr = value.split(" ");
//                for (String s : arr) {
//                    out.collect(Tuple2.of(s, 1));
//                }
//            }
//        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> mappedStream = path.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] arr = value.split(" ");
                for (String s : arr) {
                    out.collect(Tuple2.of(s, 1));
                }
            }
        });
        // keyBy   shuffle
        KeyedStream<Tuple2<String, Integer>, String> keyStream = mappedStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //reduce

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });

//        输出结果
        result.print();
//        运行环境
        env.execute();

    }

}
