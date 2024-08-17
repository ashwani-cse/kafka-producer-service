package com.kafka.orders.domain;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * @author Ashwani Kumar
 * Created on 17/08/24.
 */
public record Order(Integer orderId,
                    String locationId,
                    BigDecimal finalAmount,
                    OrderType orderType,
                    List<OrderLineItem> orderLineItems,
                    LocalDateTime orderedDateTime) {
}
