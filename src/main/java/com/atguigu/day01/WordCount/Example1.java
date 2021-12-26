package com.atguigu.day01.WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * description:
 * Created by thinkpad on 2021-09-15
 */
// 从socket读取数据，计算word count
public class Example1 {
    public static void main(String[] args) throws Exception {

//        1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 使用 parameter工具提取参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        // 2.读取数据源
        DataStreamSource<String> stream = env.socketTextStream(host, port);
        //3.TODO map操作，这里使用flatMap方法
        SingleOutputStreamOperator<Tuple2<String, Integer>> mappedStream = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] arr = value.split(" ");
                for (String s : arr) {
                    out.collect(Tuple2.of(s, 1));
                }
            }
        });
        //TODO shuffle:将不同的单词发往不同的分区
        //TODO 对当前的key进行hash发往不同的分区
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mappedStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        //TODO Reduce
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            // 定义输入元素和累加器的累加规则
            // 每个key都会维护自己的累加器
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });

        //输出
        result.print();

        //TODO 执行程序
        env.execute();

    }
}
