package com.github.michael.heinecke.kafkastreams.wordcount;

import com.github.michaelheinecke.kafkastreams.wordcount.WordCount;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class WordCountTest {

    TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    private final Serde<String> stringSerde = new Serdes.StringSerde();
    private final Serde<Long> longSerde = new Serdes.LongSerde();

    // method annotated with @Before run before every test
    @Before
    public void setup() {
        // same configs as tested class
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");  // ID can be different from test class
        // bootstrap server can be anything as there's no actual connection to kafka for testing
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = new WordCount().createTopology();
        testDriver = new TopologyTestDriver(topology, config);

        // setup test topics
        inputTopic = testDriver.createInputTopic(WordCount.INPUT_TOPIC, stringSerde.serializer(),
                stringSerde.serializer());
        outputTopic = testDriver.createOutputTopic(WordCount.OUTPUT_TOPIC, stringSerde.deserializer(),
                longSerde.deserializer());
    }

    // required to clear state directory
    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void makeSureCountsAreCorrect() {
        inputTopic.pipeInput(new TestRecord<>("testing Kafka Streams"));
        assertThat("Output topic is empty", outputTopic.isEmpty(), is(false));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("testing", 1L)));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("kafka", 1L)));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("streams", 1L)));
        assertThat("Output topic is not empty", outputTopic.isEmpty(), is(true));

        inputTopic.pipeInput(new TestRecord<>("testing Kafka again"));
        assertThat("Output topic is empty", outputTopic.isEmpty(), is(false));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("testing", 2L)));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("kafka", 2L)));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("again", 1L)));
        assertThat("Output topic is not empty", outputTopic.isEmpty(), is(true));
    }

    @Test
    public void makeSureWordsBecomeLowercase() {
        inputTopic.pipeInput(new TestRecord<>("KAFKA kafka Kafka"));
        assertThat("Output topic is empty", outputTopic.isEmpty(), is(false));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("kafka", 1L)));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("kafka", 2L)));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("kafka", 3L)));
        assertThat("Output topic is not empty", outputTopic.isEmpty(), is(true));
    }
}
