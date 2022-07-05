package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;


public class WordCountApp {
    public static void main(String[] args) {
//        Logger log = LoggerFactory.getLogger(WordCountApp.class.getSimpleName());

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "Karka-Word-Count");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        StreamsBuilder builder = new StreamsBuilder();
//        These are the Stream Processors mentioned in the notes

//        This is the Source Processor
//        Get a stream from a Kafka topic
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

//        This is the start of regular processors
        KTable<String, Long> wordCounts = wordCountInput
//        Map values to lowercase
                .mapValues(value -> value.toLowerCase())

//                Split the values by " " (spaces)
                .flatMapValues(value -> Arrays.asList(value.split(" ")))

//                Select a key and apply it as the new key
                .selectKey((originalKey, word) -> word)

//                group messages by key
                .groupByKey()
//                End of regular processors

//                Count occurrences
                .count(Named.as("Counts"));

        wordCounts.toStream().to("word-count-output");

//        This link is good to understand KafkaStreams object
//        https://kafka.apache.org/23/javadoc/org/apache/kafka/streams/KafkaStreams.html
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.start();

//        Printing topology
        System.out.println(streams.toString());


//        Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
