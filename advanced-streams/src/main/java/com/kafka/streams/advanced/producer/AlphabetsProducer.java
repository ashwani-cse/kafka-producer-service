package com.kafka.streams.advanced.producer;

import com.kafka.streams.advanced.topology.ExploreJoinsOperatorsTopology;

import java.util.Map;

/**
 * @author Ashwani Kumar
 * Created on 26/08/24.
 */
public class AlphabetsProducer {

    static final ProducerUtil producerUtil = new ProducerUtil();
    static final String alphabetAbbreviation = ExploreJoinsOperatorsTopology.ALPHABET_ABBREVIATION;
    static final String alphabet = ExploreJoinsOperatorsTopology.ALPHABET;

    public static void main(String[] args) {


        case1_KStreamAndKtableBothPublishEvent();

       // case2_OnlyKStreamPublishEvent();

       // case3_OnlyKTablePublishEvent();
    }

    private static void case1_KStreamAndKtableBothPublishEvent() {
        Map<String, String> abbreviations = Map.of(
                "A", "Apple",
                "B", "Ball",
                "C", "Cat"
        );

        Map<String, String> alphabets = Map.of(
                "A", "A is the First letter of English Alphabet",
                "B", "B is the Second letter of English Alphabet"
                //"C", "C is the Third letter of English Alphabet"
        );

        abbreviations.forEach((key, value) -> {
            //producerUtil.publishMessageSync(alphabetAbbreviation, key, value);
        });
        alphabets.forEach((key, value) -> {
            producerUtil.publishMessageSync(alphabet, key, value);
            //producerUtil.publishMessageSyncWithDelay(alphabet, key, value, -5);
        });
    }

    private static void case2_OnlyKStreamPublishEvent() {
        // if you publish the same key again, the abbreviation value will be updated in joined stream like below
        // [alphabet-with-abbreviation]: A, Alphabet[abbreviation=Airplane, description=A is the First letter of English Alphabet]
        // [alphabet-with-abbreviation]: B, Alphabet[abbreviation=Bat, description=B is the Second letter of English Alphabet]
        Map<String, String> abbreviations = Map.of(
                "A", "Airplane",
                "B", "Bat"
        );

        abbreviations.forEach((key, value) -> {
            producerUtil.publishMessageSync(alphabetAbbreviation, key, value);
        });
    }

    private static void case3_OnlyKTablePublishEvent() {
        Map<String, String> alphabets = Map.of(
                "A", "A is the First letter",
                "B", "B is the Second letter"
        );

        alphabets.forEach((key, value) -> {
            producerUtil.publishMessageSync(alphabet, key, value);
        });
    }


}
