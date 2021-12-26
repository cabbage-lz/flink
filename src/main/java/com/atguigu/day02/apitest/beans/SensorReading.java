package com.atguigu.day02.apitest.beans;

/**
 * description:
 * Created by thinkpad on 2021-09-16
 */

//TODO 传感器温度读数的数类
public class SensorReading {

    //    id ，date ，温度
    private String id;
    private Long timestamp;
    private Double temperate;

    public SensorReading() {
    }

    public SensorReading(String id, Long timestamp, Double temperate) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperate = temperate;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperate() {
        return temperate;
    }

    public void setTemperate(Double temperate) {
        this.temperate = temperate;
    }

    @Override
    public String toString() {
        return "SourceTest1_Collection{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperate=" + temperate +
                '}';
    }

}
