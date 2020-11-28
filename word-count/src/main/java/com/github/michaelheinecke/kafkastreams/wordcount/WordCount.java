package com.github.michaelheinecke.kafkastreams.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCount {

    public static final String INPUT_TOPIC = "word-count-input";
    public static final String OUTPUT_TOPIC = "word-count-output";

    public Topology createTopology(){

        StreamsBuilder builder = new StreamsBuilder();

        // 1 - stream from Kafka
        KStream<String, String> textLines = builder.stream(INPUT_TOPIC);
        KTable<String, Long> wordCount = textLines
                // 2 - map values to lowercase
                .mapValues(value -> value.toLowerCase())
                // 3 - flatmap values split by space
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                // 4 - select key to apply a new key
                .selectKey((key, word) -> word)
                // 5 - group by key before aggregation
                .groupByKey()
                // 6 - count word occurrences
                .count(Materialized.as("counts"));

        // 7 - to in order to write the results back to kafka
        wordCount.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        WordCount wordCount = new WordCount();

        KafkaStreams streams = new KafkaStreams(wordCount.createTopology(), config);
        streams.start();

        // print topology
        System.out.println(streams.toString());

        // shutdown hook to shut down streams application gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
