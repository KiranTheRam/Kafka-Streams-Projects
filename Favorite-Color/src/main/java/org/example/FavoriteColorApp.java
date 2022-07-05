package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;


public class FavoriteColorApp {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "Favorite-Color-App");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

//        Get a stream from a Kafka topic
        KStream<String, String> favoriteColorInput = builder.stream("favorite-color-input");

//        This is the start of regular processors
        KStream<String, String> usernamesAndColors = favoriteColorInput

//                Ensure message has a comma so it can be split
                .filter((key, value) -> value.contains(","))

//                Takes all text before the "," makes it lowercase, and makes that value the key
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())

//                Take the values after the "," make it lowercase, and make that the new value
                .mapValues(value -> value.split(",")[1].toLowerCase())

//                Ensures the value or color is matching one of the following
                .filter((user, color) -> Arrays.asList("green", "blue", "red").contains(color));
//                .filter((key, value) -> value.equals("red") || value.equals("blue") || value.equals("green"))

//        Sending the new stream to kafka under a new topic
        usernamesAndColors.to("usernames-and-colors");

//        We are using Method 2 of conversion: sending the information as a stream to Kafka, then reading it back as a table
//        This is done so that updates are read properly, aka the latest color value a person gave is the only information we have
        KTable<String, String> usersAndColorsTable = builder.table("usernames-and-colors");

//        Counting the occurrences of each color
        KTable<String, Long> colorCounts = usersAndColorsTable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count(Named.as("ColorCounts"));

        colorCounts.toStream().to("favorite-color-output");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

//        Clean up is usually a development thing, not for prod
        streams.cleanUp();

        streams.start();

//        Printing topology
        System.out.println(streams);

//        Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
