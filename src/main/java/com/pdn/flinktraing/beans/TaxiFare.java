package com.pdn.flinktraing.beans;

import java.math.BigInteger;

/**
 * @author zhihu
 */
public class TaxiFare {
    private String driverId ;
    private float fares;
    private Long timeStamp;

    public TaxiFare(String driverId, float fares, Long timeStamp) {
        this.driverId = driverId;
        this.fares = fares;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "TaxiFare{" +
                "driverId='" + driverId + '\'' +
                ", fares=" + fares +
                ", timeStamp=" + timeStamp +
                '}';
    }

    public String getDriverId() {
        return driverId;
    }

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }

    public float getFares() {
        return fares;
    }

    public void setFares(float fares) {
        this.fares = fares;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
