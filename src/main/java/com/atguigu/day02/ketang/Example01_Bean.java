package com.atguigu.day02.ketang;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

/**
 * description:TODO  自定义数据源
 * Created by thinkpad on 2021-09-17
 */
public class Example01_Bean {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.print();
        env.execute();

    }

    public static class ClickSource implements SourceFunction<Event> {
        //声明user 和url数组
        private boolean running = true;
        private Random random = new Random();
        private String[] userAll = {"tom", "james", "jerry", "jane", "tim"};
        private String[] urlArr = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        // flink run ***.jar 触发事件执行
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            while (running) {
                ctx.collect(Event.of(
//随机定义的user 和url
                        userAll[random.nextInt(userAll.length)],
                        urlArr[random.nextInt(urlArr.length)],
                        Calendar.getInstance().getTimeInMillis()
                ));
                Thread.sleep(10L);
            }
        }

        // flink cancel jobId时触发cancel的执行
        @Override
        public void cancel() {
            running = false;
        }
    }

    //TODO  POJO类
    // 1. 必须是公有类
    // 2. 所有字段必须是公有的
    // 3. 必须有空构造器
    public static class Event {
        public String user;
        public String url;
        public Long timestamp;

//        定义构造器

        public Event() {
        }

        public Event(String user, String url, Long timestamp) {
            this.user = user;
            this.url = url;
            this.timestamp = timestamp;
        }

        //of 实例化event
        public static Event of(String user, String url, Long timestamp) {
            return new Event(user, url, timestamp);
        }

        @Override
//        new Timestamp 本地时间戳
        public String toString() {
            String s = "Event{" +
                    "user='" + user + '\'' +
                    ", url='" + url + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
            return s;
        }
    }
}
