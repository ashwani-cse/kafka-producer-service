package com.kafka.orders.domain;

import java.math.BigDecimal;

/**
 * @author Ashwani Kumar
 * Created on 18/08/24.
 */
public record Revenue(String locationId,
                      BigDecimal finalAmount) {
}
