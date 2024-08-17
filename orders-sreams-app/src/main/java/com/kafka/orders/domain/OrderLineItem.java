package com.kafka.orders.domain;

import java.math.BigDecimal;

/**
 * @author Ashwani Kumar
 * Created on 17/08/24.
 */
public record OrderLineItem(String item, int count, BigDecimal amount) {
}
