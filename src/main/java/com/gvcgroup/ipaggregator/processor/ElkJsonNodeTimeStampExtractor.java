package com.gvcgroup.ipaggregator.processor;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Calendar;
import java.util.Date;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ferdinand Holzer
 */
public class ElkJsonNodeTimeStampExtractor implements TimestampExtractor {
    private static final Logger log = LoggerFactory.getLogger(ElkJsonNodeTimeStampExtractor.class);
    private static final String ERROR_FAILED_PARSE = "Failed to parse record's timestamp.";

    @Override
    public long extract(ConsumerRecord<Object, Object> cr, long l) {
        try {
            String sDate = ((ObjectNode)cr.value()).get("@timestamp").asText();
            Calendar cDate = javax.xml.bind.DatatypeConverter.parseDateTime(sDate);
            return cDate.getTimeInMillis();
        } catch(Exception ex) {
            log.error(ERROR_FAILED_PARSE);
            return new Date().getTime();
        }
    }

}
