package com.gvcgroup.ipaggregator.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.json.JsonDeserializer;

/**
 *
 * @author Ferdinand Holzer
 */
public class ObjectNodeDeserializer implements Deserializer<ObjectNode> {
    private static final String ERROR_ROOT_NOT_OBJECT = "Root is not an object.";
    private final JsonDeserializer deser = new JsonDeserializer();

    @Override
    public void configure(Map<String, ?> map, boolean bln) {
        deser.configure(map, bln);
    }

    @Override
    public ObjectNode deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;
        
        JsonNode root = deser.deserialize(topic, bytes);
        if(!root.isObject()) {
            throw new SerializationException(ERROR_ROOT_NOT_OBJECT);
        }
        return (ObjectNode)root;
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
