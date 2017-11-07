package com.gvcgroup.ipaggregator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gvcgroup.ipaggregator.model.Isp;
import com.gvcgroup.ipaggregator.processor.ActualClientIpValueMapper;
import com.gvcgroup.ipaggregator.processor.ElkJsonNodeTimeStampExtractor;
import com.gvcgroup.ipaggregator.processor.IisLogValueMapper;
import com.gvcgroup.ipaggregator.processor.IpAggregationResultKeyValueMapper;
import com.gvcgroup.ipaggregator.processor.IspAggregationResultKeyValueMapper;
import com.gvcgroup.ipaggregator.processor.IspKeyValueMapper;
import com.gvcgroup.ipaggregator.processor.JsonStringKeyValueMapper;
import com.gvcgroup.ipaggregator.serialization.IspDeserializer;
import com.gvcgroup.ipaggregator.serialization.IspSerializer;
import com.gvcgroup.ipaggregator.serialization.ObjectNodeDeserializer;
import com.gvcgroup.ipaggregator.serialization.ObjectNodeSerializer;
import java.io.IOException;
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
    private static final long TIMEWINDOW_SIZE = 1000;
    private static final String TOPIC_OUTPUT_LOGAGG_IP = "logagg.ip.test";
    private static final String TOPIC_OUTPUT_LOGAGG_ISP = "logagg.isp.test";
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
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
        final Serde<Isp> ispSerde = Serdes.serdeFrom(new IspSerializer(), new IspDeserializer());

        // In the subsequent lines we define the processing topology of the Streams application.
        final KStreamBuilder builder = new KStreamBuilder();


        // Build streams

        final KStream<String, ObjectNode> textLines = builder.stream(new ElkJsonNodeTimeStampExtractor(), stringSerde, objectNodeSerde, new String[] { TOPIC_INPUT });

        // TODO filter events that don't have a message key => parse error

        // remove comment lines
        KStream<String, ObjectNode> filtered = textLines.filterNot((String k, ObjectNode v) -> v.get("message").asText().startsWith("#"));

        // parse log line
        KStream<String, ObjectNode> parsed = filtered.mapValues(new IisLogValueMapper());

        // evaluate actual client ip address
        KStream<String, ObjectNode> ipEvaluated = parsed.mapValues(new ActualClientIpValueMapper());


        // Aggregate based on IP

        KTable<Windowed<String>, Long> aggByIpTable = ipEvaluated
                .groupBy(new JsonStringKeyValueMapper(ActualClientIpValueMapper.FIELDNAME_ACTUALREMOTEADDR), stringSerde, objectNodeSerde)
                .count(TimeWindows.of(TIMEWINDOW_SIZE));

        KStream<String, ObjectNode> aggByIpStream = aggByIpTable.toStream()
                //.map((Windowed<String> k, Long v) -> new KeyValue<>(k.key() + "@" + k.window().start(), v))
                .map(new IpAggregationResultKeyValueMapper("all"))
                .through(stringSerde, objectNodeSerde, TOPIC_OUTPUT_LOGAGG_IP);

        aggByIpStream.print();


        // Aggregate based on ISP

        IspKeyValueMapper ispMapper = new IspKeyValueMapper(ActualClientIpValueMapper.FIELDNAME_ACTUALREMOTEADDR);

        KTable<Windowed<Isp>, Long> aggByIspTable = ipEvaluated
                .groupBy(ispMapper, ispSerde, objectNodeSerde)
                .count(TimeWindows.of(TIMEWINDOW_SIZE));

        KStream<String, ObjectNode> aggByIspStream = aggByIspTable.toStream()
                .map(new IspAggregationResultKeyValueMapper())
                .through(stringSerde, objectNodeSerde, TOPIC_OUTPUT_LOGAGG_ISP);

        aggByIspStream.print();


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
