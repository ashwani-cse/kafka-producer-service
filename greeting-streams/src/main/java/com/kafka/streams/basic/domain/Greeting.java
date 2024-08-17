package com.kafka.streams.basic.domain;

import java.time.LocalDateTime;

/**
 * @author Ashwani Kumar
 * Created on 13/08/24.
 */
public record Greeting(String message,
                       LocalDateTime timeStamp) {
}
