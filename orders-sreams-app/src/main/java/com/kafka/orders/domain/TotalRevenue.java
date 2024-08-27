package com.kafka.orders.domain;

import java.math.BigDecimal;

/**
 * @author Ashwani Kumar
 * Created on 17/08/24.
 */
public record TotalRevenue(String locationId,
                           Integer runningOrderCount,
                           BigDecimal runningRevenue) {
    public TotalRevenue(){
        this("", 0, BigDecimal.ZERO);
    }

    public TotalRevenue updateRunningRevenue(String key, Order value) {
        return new TotalRevenue(key, runningOrderCount + 1, runningRevenue.add(value.finalAmount()));
    }
}
