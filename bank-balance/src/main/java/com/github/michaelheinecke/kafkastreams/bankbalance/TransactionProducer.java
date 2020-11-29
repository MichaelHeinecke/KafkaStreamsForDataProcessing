package com.github.michaelheinecke.kafkastreams.bankbalance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TransactionProducer {
    private static final Logger logger = LoggerFactory.getLogger(TransactionProducer.class.getName());
    private static final Random random = new Random();
    // names for Transactions and ProducerRecord key
    private static final String[] names = {"Adalbert", "Mildrid", "Herman", "Sextus", "Augustus", "Cleopatra"};
    // object to JSON
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public TransactionProducer() {
    }

    public static void main(String[] args) {
        new TransactionProducer().run();
    }

    public void run() {
        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook called");
            logger.info("Closing producer");
            producer.close();
            logger.info("Application shut down gracefully");
        }));

        // produce data in endless loop
        while (true) {
            // pick random name from names array
            String name = names[random.nextInt(names.length)];

            // produce ~10 messages per second
            try {
                producer.send(createRandomTransaction(name));
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
        producer.close();
    }

    private ProducerRecord<String, String> createRandomTransaction(String name) {
        String json = "";
        // get random amount between 0 and 99
        Integer amount = random.nextInt(100);

        Transaction transaction = new Transaction(name, amount, Instant.now().toString());

        // create json string from transaction object
        try {
            json = objectMapper.writeValueAsString(transaction);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        logger.info(json);

        return new ProducerRecord<>("bank_balance_input", name, json);
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServer = "localhost:9092";

        // set producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // add properties to make producer safe
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // below properties are implicit with ENABLE_IDEMPOTENCE_CONFIG set to true; added for clarity
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        return new KafkaProducer<>(properties);
    }
}
