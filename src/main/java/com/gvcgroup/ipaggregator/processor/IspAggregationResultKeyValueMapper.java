package com.gvcgroup.ipaggregator.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gvcgroup.ipaggregator.model.Isp;
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
public class IspAggregationResultKeyValueMapper implements KeyValueMapper<Windowed<Isp>, Long, KeyValue<String, ObjectNode>> {
    private static final String FIELDNAME_TS = "@timestamp";
    private static final String FIELDNAME_ASN = "asn";
    private static final String FIELDNAME_ASO = "aso";
    private static final String FIELDNAME_ISP = "isp";
    private static final String FIELDNAME_ORG = "org";
    private static final String FIELDNAME_COUNT = "count";
    private final ObjectMapper mapper;

    public IspAggregationResultKeyValueMapper() {
        this.mapper = new ObjectMapper();
    }

    @Override
    public KeyValue<String, ObjectNode> apply(Windowed<Isp> k, Long v) {
        ObjectNode root = mapper.createObjectNode();
        root.put(FIELDNAME_TS, ZonedDateTime.ofInstant(Instant.ofEpochMilli(k.window().start()), ZoneId.of("UTC"))
                .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        Isp i = k.key();
        root.put(FIELDNAME_ASN, i.getAsn());
        root.put(FIELDNAME_ASO, i.getAso());
        root.put(FIELDNAME_ISP, i.getIsp());
        root.put(FIELDNAME_ORG, i.getOrg());
        root.put(FIELDNAME_COUNT, v);
        return new KeyValue<>(null, root);
    }

}
