package com.gvcgroup.ipaggregator.processor;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 *
 * @author storm
 */
public class LogAppenTimestampTransformerSupplier implements ValueTransformerSupplier<ObjectNode, ObjectNode> {

    @Override
    public ValueTransformer<ObjectNode, ObjectNode> get() {
        return new ValueTransformer<ObjectNode, ObjectNode>() {
            private ProcessorContext pc;

            @Override
            public void init(ProcessorContext pc) {
                this.pc = pc;
            }

            @Override
            public ObjectNode transform(ObjectNode v) {
                v.put("appendTs", pc.timestamp());
                return v;
            }

            @Override
            public ObjectNode punctuate(long l) {
                return null;
            }

            @Override
            public void close() {
            }
        };
    }

}
