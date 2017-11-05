package com.gvcgroup.ipaggregator.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 *
 * @author Ferdinand Holzer
 */
public class JsonNodeSerializer implements Serializer<JsonNode> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean bln) {
    }

    @Override
    public byte[] serialize(String string, JsonNode obj) {
        try {
            return objectMapper.writeValueAsBytes(obj);
        } catch(Exception ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
