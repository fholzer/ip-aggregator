package com.gvcgroup.ipaggregator.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 *
 * @author Ferdinand Holzer
 */
public class JsonNodeDeserializer implements Deserializer<JsonNode> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean bln) {
    }

    @Override
    public JsonNode deserialize(String string, byte[] bytes) {
        try {
            return objectMapper.readTree(bytes);
        } catch(Exception ex) {
            throw new SerializationException(ex);
        }
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
