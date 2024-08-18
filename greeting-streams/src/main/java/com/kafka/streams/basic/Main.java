package com.kafka.streams.basic;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Semaphore;

import static java.lang.Thread.sleep;

@Slf4j
public class Main {
    public static void main(String[] args) {

        System.out.printf("Hello and welcome!");
        Semaphore semaphore = new Semaphore(2);

        Runnable runnable = () -> {
            try {
                log.info("++++++" + Thread.currentThread().getName() + " is waiting to acquire semaphore.");
                semaphore.acquire();  // Acquire a permit before running
                log.info("###### " + Thread.currentThread().getName() + " running...");

                sleep(20000);
            } catch (InterruptedException e) {
                log.error("InterruptedException in : {}  ", e.getMessage());
            }finally {
                log.info("------" + Thread.currentThread().getName() + " releasing semaphore...");
                semaphore.release(); // Release the permit
            }
        };

        for (int i = 1; i <= 5; i++) {
            Thread th = new Thread(runnable, "Thread-" + i);
            log.info("Thread-" + i + " ready to acquire semaphore and start");
            th.start();
        }
    }
}