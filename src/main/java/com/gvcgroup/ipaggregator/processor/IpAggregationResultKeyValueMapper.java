package com.gvcgroup.ipaggregator.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

/**
 *
 * @author Ferdinand Holzer
 */
public class IpAggregationResultKeyValueMapper implements KeyValueMapper<Windowed<String>, Long, KeyValue<String, ObjectNode>> {
    private static final String FIELDNAME_TS = "@timestamp";
    private static final String FIELDNAME_SERVICE = "service";
    private static final String FIELDNAME_IP = "ipaddr";
    private static final String FIELDNAME_COUNT = "count";
    private final ObjectMapper mapper;
    private final String serviceName;

    public IpAggregationResultKeyValueMapper(String serviceName) {
        this.mapper = new ObjectMapper();
        this.serviceName = serviceName;
    }

    @Override
    public KeyValue<String, ObjectNode> apply(Windowed<String> k, Long v) {
        ObjectNode root = mapper.createObjectNode();
        root.put(FIELDNAME_TS, ZonedDateTime.ofInstant(Instant.ofEpochMilli(k.window().start()), ZoneId.of("UTC"))
                .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        root.put(FIELDNAME_SERVICE, this.serviceName);
        root.put(FIELDNAME_IP, k.key());
        root.put(FIELDNAME_COUNT, v);
        return new KeyValue<>(null, root);
    }

}
