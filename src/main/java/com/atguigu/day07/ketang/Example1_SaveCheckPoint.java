package com.atguigu.day07.ketang;

import com.atguigu.day02.ketang.Example01_Bean;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description:
 * Created by thinkpad on 2021-09-25
 */
public class Example1_SaveCheckPoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10 * 1000L);
        env.setStateBackend(new FsStateBackend("file:\\F:\\0428bigdate\\01java\\bigdatapro\\flink\\src\\main\\resources\\ckpt"));

        env
                .addSource(new Example01_Bean.ClickSource())
                .print();
        env.execute();

    }
}
