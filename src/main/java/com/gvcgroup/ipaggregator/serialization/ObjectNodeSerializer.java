package com.gvcgroup.ipaggregator.serialization;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonSerializer;

/**
 *
 * @author Ferdinand Holzer
 */
public class ObjectNodeSerializer implements Serializer<ObjectNode> {
    private final JsonSerializer ser = new JsonSerializer();

    @Override
    public void configure(Map<String, ?> map, boolean bln) {
        ser.configure(map, bln);
    }

    @Override
    public byte[] serialize(String topic, ObjectNode obj) {
        return ser.serialize(topic, obj);
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
