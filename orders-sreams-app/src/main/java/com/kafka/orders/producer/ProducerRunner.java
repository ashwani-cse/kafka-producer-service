package com.kafka.orders.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafka.orders.domain.Order;
import com.kafka.orders.domain.OrderLineItem;
import com.kafka.orders.domain.OrderType;
import com.kafka.orders.topology.OrdersTopology;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static java.lang.Thread.sleep;

/**
 * @author Ashwani Kumar
 * Created on 27/07/24.
 */
@Slf4j
public class ProducerRunner {
    private static final Logger logger = LoggerFactory.getLogger(ProducerRunner.class.getName());

    private static ProducerUtil producerUtil = new ProducerUtil();

    private static ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    public static void main(String[] args) throws InterruptedException {

        publishOrders(objectMapper, buildOrders());
        //publishBulkOrders(objectMapper);
        producerUtil.closeProducer();
    }

    private static void publishBulkOrders(ObjectMapper objectMapper) throws InterruptedException {

        int count = 0;
        while (count < 100) {
            var orders = buildOrders();
            publishOrders(objectMapper, orders);
            sleep(1000);
            count++;
        }
    }

    private static void publishOrders(ObjectMapper objectMapper, List<Order> orders) {

        orders
                .forEach(order -> {
                    try {
                        var ordersJSON = objectMapper.writeValueAsString(order);
                        var recordMetaData = producerUtil.publishMessageSync(OrdersTopology.ORDERS_TOPIC, order.orderId() + "", ordersJSON);
                        log.info("Published the order message : {} ", Optional.ofNullable(recordMetaData));
                    } catch (JsonProcessingException e) {
                        log.error("JsonProcessingException : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    } catch (Exception e) {
                        log.error("Exception : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                    delay(6000);
                });
    }


    private static void delay(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static List<Order> buildOrders() {
        var orderItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );

        var orderItemsRestaurant = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        var order1 = new Order(12345, "store_1234",
               // new BigDecimal("27.00"),
                new BigDecimal("0.00"), // to test stream error
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.now()
        );

        var order2 = new Order(54321, "store_1234",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
                LocalDateTime.now()
        );

        var order3 = new Order(12345, "store_4567",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.now()
        );

        var order4 = new Order(12345, "store_4567",
                new BigDecimal("27.00"),
                OrderType.RESTAURANT,
                orderItems,
                LocalDateTime.now()
        );

        return List.of(
                order1,
                order2,
                order3,
                order4
        );
    }

}
