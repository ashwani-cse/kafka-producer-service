package com.kafka.streams.advanced.domain;

import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Ashwani Kumar
 * Created on 25/08/24.
 */
@Slf4j
public record AlphabetWordAggregate(String key,
                                    Set<String> words,
                                    int runningCount) {

    public AlphabetWordAggregate() {
        this("", new HashSet<>(), 0);
    }

    public AlphabetWordAggregate updateNewEvents(String key, String word) {
        log.info("Before update : {}", this);
        log.info("New Record : key : {}, word : {}", key, word);
        words.add(word);
        return new AlphabetWordAggregate(key, words, runningCount + 1);
    }
}
