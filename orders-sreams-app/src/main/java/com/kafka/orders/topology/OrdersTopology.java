package com.kafka.orders.topology;

import com.kafka.orders.domain.Order;
import com.kafka.orders.domain.OrderType;
import com.kafka.orders.domain.Revenue;
import com.kafka.orders.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ashwani Kumar
 * Created on 17/08/24.
 */
public class OrdersTopology {

    public static final String ORDERS_TOPIC = "orders";
    public static final String GENERAL_ORDERS_TOPIC = "general-orders";
    public static final String RESTAURANT_ORDERS_TOPIC = "restaurant-orders";

    public static final String STORES_TOPIC = "stores";
    private static final Logger log = LoggerFactory.getLogger(OrdersTopology.class);

    static Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
    static Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);
    static ValueMapper<Order, Revenue> revenueMapper = order -> new Revenue(order.locationId(), order.finalAmount());

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Order> orderKStream = streamsBuilder.stream(ORDERS_TOPIC,
                Consumed.with(Serdes.String(), SerdesFactory.orderSerdes()));

        orderKStream.print(Printed.<String, Order>toSysOut().withLabel("orderKStream"));

        orderKStream
                .split(Named.as("General-restaurant-stream"))
                .branch(generalPredicate,
                        Branched.withConsumer(generalOrdersStream -> {
                            generalOrdersStream.print(Printed.<String, Order>toSysOut().withLabel("generalOrdersStream"));
                            generalOrdersStream
                                    .mapValues(((readOnlyKey, value) -> revenueMapper.apply(value)))
                                    .to(GENERAL_ORDERS_TOPIC,
                                    Produced.with(Serdes.String(), SerdesFactory.revenueSerdes()));
                        })
                )
                .branch(restaurantPredicate,
                        Branched.withConsumer(restaurantOrdersStream -> {
                            restaurantOrdersStream.print(Printed.<String, Order>toSysOut().withLabel("restaurantOrdersStream"));
                            restaurantOrdersStream
                                    .mapValues(((readOnlyKey, value) -> revenueMapper.apply(value)))
                                    .to(RESTAURANT_ORDERS_TOPIC,
                                    Produced.with(Serdes.String(), SerdesFactory.revenueSerdes()));
                        })
                );


        return streamsBuilder.build();
    }

    public static Topology exploreStreamErrors() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Order> orderKStream = streamsBuilder.stream(ORDERS_TOPIC,
                Consumed.with(Serdes.String(), SerdesFactory.orderSerdes()));
       // Two way to handle stream runtime errors
        // 1. Using try catch block
        orderKStream
                .mapValues((readOnlyKey, value) -> {
                    if (value.finalAmount().intValue()<1) {
                        try {
                            throw new RuntimeException("Invalid final amount");
                        } catch (RuntimeException e) {
                            log.error("Error in order processing : {}", e.getMessage());
                            return null; // return null record for error case
                        }
                    }
                    return value;
                })
                .filter((key, value) -> value != null) // filter out null records
                .to("orders-destination-topic", Produced.with(Serdes.String(), SerdesFactory.orderSerdes()));

        // 2. Using custom Stream Error handler, no need to use try catch block
        orderKStream
                .mapValues((readOnlyKey, value) -> {
                    if (value.finalAmount().intValue()<1) {
                        throw new RuntimeException("Invalid final amount");
                    }
                    return value;
                })
                .to("orders-destination-topic", Produced.with(Serdes.String(), SerdesFactory.orderSerdes()));

        return streamsBuilder.build();
    }
}
