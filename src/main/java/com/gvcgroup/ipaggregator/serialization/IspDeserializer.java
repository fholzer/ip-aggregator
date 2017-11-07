package com.gvcgroup.ipaggregator.serialization;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gvcgroup.ipaggregator.model.Isp;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

/**
 *
 * @author Ferdinand Holzer
 */
public class IspDeserializer implements Deserializer<Isp> {
    private static final String FIELDNAME_ASN = "asn";
    private static final String FIELDNAME_ASO = "aso";
    private static final String FIELDNAME_ISP = "isp";
    private static final String FIELDNAME_ORG = "org";
    private final ObjectNodeDeserializer deser = new ObjectNodeDeserializer();

    @Override
    public void configure(Map<String, ?> map, boolean bln) {
        deser.configure(map, bln);
    }

    @Override
    public Isp deserialize(String string, byte[] bytes) {
        ObjectNode root = deser.deserialize(string, bytes);
        Isp i = new Isp(
                root.get(FIELDNAME_ASN).asInt(),
                root.get(FIELDNAME_ASO).asText(),
                root.get(FIELDNAME_ISP).asText(),
                root.get(FIELDNAME_ORG).asText()
        );
        return i;
    }

    @Override
    public void close() {
    }

}
