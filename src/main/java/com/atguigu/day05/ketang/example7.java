package com.atguigu.day05.ketang;

import com.atguigu.day01.WordCount.Example1;
import com.atguigu.day02.ketang.Example01_Bean;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * description:
 * Created by thinkpad on 2021-09-24
 */
public class example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Example01_Bean.Event> stream = env.addSource(new Example01_Bean.ClickSource());

        // 查询流
        DataStreamSource<String> queryStream = env.socketTextStream("hadoop102", 9999);

        stream
                .keyBy(r -> r.user)
                .connect(queryStream.setParallelism(1).broadcast())
                .flatMap(new CoFlatMapFunction<Example01_Bean.Event, String, Example01_Bean.Event>() {
                    private String query = "";

                    @Override
                    public void flatMap1(Example01_Bean.Event value, Collector<Example01_Bean.Event> out) throws Exception {
                        if (value.url.equals(query)) out.collect(value);
                    }

                    @Override
                    public void flatMap2(String value, Collector<Example01_Bean.Event> out) throws Exception {
                        query = value;
                    }
                })
                .print();

        env.execute();

    }
}
