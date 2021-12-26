package com.atguigu.day02.ketang;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.graph.StreamEdge;
import redis.clients.jedis.Jedis;

/**
 * description:
 * Created by thinkpad on 2021-09-17
 */
public class Example13_ToRedis {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new Example01_Bean.ClickSource())
                .addSink(new RichSinkFunction<Example01_Bean.Event>() {
                    private Jedis jedis;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        jedis = new Jedis("hadoop102");

                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        jedis.close();

                    }

                    @Override
                    public void invoke(Example01_Bean.Event value, Context context) throws Exception {
                        super.invoke(value, context);
                        jedis.set(value.user, value.url);
                    }
                });

        env.execute();

    }
}
