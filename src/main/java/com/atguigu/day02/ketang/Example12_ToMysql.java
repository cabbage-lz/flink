package com.atguigu.day02.ketang;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.graph.StreamEdge;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * description:
 * Created by thinkpad on 2021-09-17
 */
public class Example12_ToMysql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new Example01_Bean.ClickSource())
                .addSink(new RichSinkFunction<Example01_Bean.Event>() {
                    private Connection conn;
                    private PreparedStatement insertStmt;
                    private PreparedStatement updateStmt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
//                        TODO 获取mysql连接
                        Class.forName("com.mysql.jdbc.Driver");
                        conn = DriverManager.getConnection(
                                "jdbc:mysql://8.142.13.84:3306/sensor",
                                "root",
                                "123456"
                        );
                        insertStmt = conn.prepareStatement("INSERT INTO clicks(user, url) VALUES (?, ?) ");
                        updateStmt = conn.prepareStatement("update clicks set url = ? where user = ?");
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        //                        关闭连接
                        insertStmt.close();
                        updateStmt.close();
                        conn.close();
                    }

                    @Override
                    public void invoke(Example01_Bean.Event value, Context context) throws Exception {
                        super.invoke(value, context);
//TODO 写入
                        updateStmt.setString(1, value.url);
                        updateStmt.setString(2, value.user);
                        updateStmt.execute();

                        if (updateStmt.getUpdateCount() == 0) {
                            insertStmt.setString(1, value.user);
                            insertStmt.setString(2, value.url);
                            insertStmt.execute();
                        }
                    }
                });

        env.execute();
    }

}
