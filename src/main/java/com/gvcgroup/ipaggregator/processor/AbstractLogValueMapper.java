package com.gvcgroup.ipaggregator.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.kstream.ValueMapper;

/**
 *
 * @author storm
 */
public abstract class AbstractLogValueMapper implements ValueMapper<ObjectNode, ObjectNode> {
    public static final String FIELDNAME_TAG = "tag";
    public static final String TAG_PARSE_ERROR = "parseerror";


    protected void addTag(ObjectNode parent, String tagName) {
        if(parent.has(FIELDNAME_TAG)) {
            JsonNode n = parent.get(FIELDNAME_TAG);
            ArrayNode a;
            if(!n.isArray()) {
                a = parent.arrayNode();
                a.add(n);
                a.add(tagName);
                parent.set(FIELDNAME_TAG, a);
            } else {
                a = (ArrayNode)n;
                a.add(tagName);
            }
            return;
        }
        parent.put(FIELDNAME_TAG, tagName);
    }
}
