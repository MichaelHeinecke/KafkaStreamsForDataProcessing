package com.github.michaelheinecke.kafkastreams.favouritecolour;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavouriteColour {

    public Topology createTopology(){

        StreamsBuilder builder = new StreamsBuilder();

        List<String> validColours = Arrays.asList("red", "green", "blue");

        // read stream from Kafka
        KStream<String, String> textLines = builder.stream("favourite-colour-input");

        KStream<String, String> usersAndColours = textLines
                // filter bad data; expected format "user_name,colour"
                .filter((key ,value) -> value.matches("\\w+,\\w+"))
                // select user as key
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                // select colour as value
                .mapValues(value -> value.split(",")[1].toLowerCase())
                // filter out colours other than green, blue, red
                .filter((name, colour) -> validColours.contains(colour));

        // write to intermediary topic
        usersAndColours.to("user-keys-and-colours");

        // read topic as KTable to read updates for existing users correctly
        KTable<String, String> usersAndColoursTable = builder.table("user-keys-and-colours");

        // count occurrences of colours
        KTable<String, Long> colourCount = usersAndColoursTable
                // group by colour
                .groupBy((user, colour) -> new KeyValue<>(colour, colour))
                .count(Materialized.as("counts"));

        // 7 - to in order to write the results back to kafka
        colourCount.toStream().to("favourite-colour-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // disable cache to demonstrate all the steps in transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        FavouriteColour favouriteColour = new FavouriteColour();

        KafkaStreams streams = new KafkaStreams(favouriteColour.createTopology(), config);

        // default cleanup only in dev
        streams.cleanUp();
        streams.start();

        // print topology
        System.out.println(streams.toString());

        // shutdown hook to shut down streams application gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
