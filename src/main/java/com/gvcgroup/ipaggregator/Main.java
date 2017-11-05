package com.gvcgroup.ipaggregator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gvcgroup.ipaggregator.processor.ActualClientIpValueMapper;
import com.gvcgroup.ipaggregator.processor.ElkJsonNodeTimeStampExtractor;
import com.gvcgroup.ipaggregator.processor.IisLogValueMapper;
import com.gvcgroup.ipaggregator.processor.IpAggregationResultKeyValueMapper;
import com.gvcgroup.ipaggregator.processor.JsonStringKeyValueMapper;
import com.gvcgroup.ipaggregator.serialization.ObjectNodeDeserializer;
import com.gvcgroup.ipaggregator.serialization.ObjectNodeSerializer;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

/**
 *
 * @author Ferdinand Holzer
 */
public class Main {
    private static final String TOPIC_INPUT = "iislogs.raw";
    private static final String TOPIC_OUTPUT_PARSED = "iislogs.parsed";
    private static final String TOPIC_OUTPUT_IPAGG = "ipagg.test";
    private static final long TIMEWINDOWSIZE = 100;
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "ip-aggregator");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "ip-aggregator-1");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 60 * 1000);
        // For illustrative purposes we disable record caches
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        final Serde<ObjectNode> objectNodeSerde = Serdes.serdeFrom(new ObjectNodeSerializer(), new ObjectNodeDeserializer());
        final Serde<String> stringSerde = Serdes.String();

        // In the subsequent lines we define the processing topology of the Streams application.
        final KStreamBuilder builder = new KStreamBuilder();


        final KStream<String, ObjectNode> textLines = builder.stream(new ElkJsonNodeTimeStampExtractor(), stringSerde, objectNodeSerde, new String[] { TOPIC_INPUT });

        KStream<String, ObjectNode> filtered = textLines.filterNot((String k, ObjectNode v) -> v.get("message").asText().startsWith("#"));
        KStream<String, ObjectNode> parsed = filtered.mapValues(new IisLogValueMapper());

        KTable<Windowed<String>, Long> agg = parsed.through(stringSerde, objectNodeSerde, TOPIC_OUTPUT_PARSED)
                .mapValues(new ActualClientIpValueMapper())
                .groupBy(new JsonStringKeyValueMapper(ActualClientIpValueMapper.FIELDNAME_ACTUALREMOTEADDR), stringSerde, objectNodeSerde)
                .count(TimeWindows.of(TIMEWINDOWSIZE));

        KStream<String, ObjectNode> aggStream = agg.toStream()
                //.map((Windowed<String> k, Long v) -> new KeyValue<>(k.key() + "@" + k.window().start(), v))
                .map(new IpAggregationResultKeyValueMapper("all"))
                .through(stringSerde, objectNodeSerde, TOPIC_OUTPUT_IPAGG);

        aggStream.print();

        /*
         * We'll need a dedicated branch per service.
         * Can't send historic data to kafka, because we can't process original
         *     event timestamp in kafka. This is by design. Events may be
         *     ingested and processed out of order otherwise, this would hinder
         *     us doing windowed aggregations.
         */

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
