package com.abhirockzz;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;


public class KafkaStreamsApp {

    static String INPUT_TOPIC = "keys";
    static String OUTPUT_TOPIC = "counts";
    static String KAFKA_BROKER = "localhost:9092";
    static String APP_ID = "counts-app";
    static String STATE_STORE_DIR = "/data/count-store";
    static String STATE_STORE_NAME = "count-store";

    public static void main(String[] args) throws InterruptedException {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC); //key: foo, value:bar
        inputStream.groupByKey()
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STATE_STORE_NAME)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                //with mapValues, we are converting Long (count) to string so that it can be seen easily in CLI
                .mapValues(new ValueMapper<Long, String>() {
                    @Override
                    public String apply(Long v) {
                        return String.valueOf(v);
                    }
                })
                .to(OUTPUT_TOPIC);

        Topology topology = builder.build();
        KafkaStreams streamsApp = new KafkaStreams(topology, getKafkaStreamsConfig());
        streamsApp.start();
        System.out.println("Started Kafka streams app..");

        Runtime.getRuntime().addShutdownHook(new Thread(streamsApp::close));
        new CountDownLatch(1).await();
    }

    static Properties getKafkaStreamsConfig() {
        Properties configurations = new Properties();
        String kafkaBroker = System.getenv().get("KAFKA_BROKER");
        System.out.println("Kafka Broker " + kafkaBroker);
        configurations.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker + ":9092");
        configurations.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        configurations.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        configurations.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        configurations.put(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
        configurations.put(StreamsConfig.RETRY_BACKOFF_MS_CONFIG, "500");
        configurations.put(StreamsConfig.STATE_DIR_CONFIG, STATE_STORE_DIR); // /data/count-store

        return configurations;
    }

}
