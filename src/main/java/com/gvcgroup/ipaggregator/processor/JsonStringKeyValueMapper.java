package com.gvcgroup.ipaggregator.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.kstream.KeyValueMapper;

/**
 *
 * @author Ferdinand Holzer
 */
public class JsonStringKeyValueMapper implements KeyValueMapper<Object, ObjectNode, String> {
    private final String fieldName;

    public JsonStringKeyValueMapper(String fieldName) {
        this.fieldName = fieldName;
    }
    
    @Override
    public String apply(Object k, ObjectNode v) {
        JsonNode n = v.get(this.fieldName);
        if(n == null) {
            System.out.println("Field '" + fieldName + "' not found in object: " + v.toString());
            return null;
        }
        if(!n.isTextual()) {
            System.out.println("Field '" + fieldName + "' not textual in object: " + v.toString());
            return null;
        }
        return n.asText();
    }
}
