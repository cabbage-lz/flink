package com.atguigu.day01.WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * description:
 * Created by thinkpad on 2021-09-15
 */
// 从socket读取数据，计算word count
public class Example2 {
    public static void main(String[] args) throws Exception {

//        1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        // 2.读取数据源
        String inpath = "F:\\0428bigdate\\01java\\bigdatapro\\flink\\src\\main\\resources\\hello.txt";
        DataSource<String> inputDataSet = env.readTextFile(inpath);

        //3.对数据集进行处理 ，按空格切开转换成（word,1）
        AggregateOperator<Tuple2<String, Integer>> sum = inputDataSet.flatMap(new MyflatMapper())
                .groupBy(0)
                .sum(1);
        sum.print();


    }

    //自定义类实现
    public static class MyflatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按空格分词
            String[] words = value.split(" ");
            //遍历所有word
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
