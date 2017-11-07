package com.gvcgroup.ipaggregator.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gvcgroup.ipaggregator.model.Isp;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

/**
 *
 * @author Ferdinand Holzer
 */
public class IspSerializer implements Serializer<Isp> {
    private static final String FIELDNAME_ASN = "asn";
    private static final String FIELDNAME_ASO = "aso";
    private static final String FIELDNAME_ISP = "isp";
    private static final String FIELDNAME_ORG = "org";
    private final ObjectNodeSerializer ser = new ObjectNodeSerializer();
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean bln) {
        ser.configure(map, bln);
    }

    @Override
    public byte[] serialize(String string, Isp t) {
        ObjectNode root = mapper.createObjectNode();
        root.put(FIELDNAME_ASN, t.getAsn());
        root.put(FIELDNAME_ASO, t.getAso());
        root.put(FIELDNAME_ISP, t.getIsp());
        root.put(FIELDNAME_ORG, t.getOrg());
        return ser.serialize(string, root);
    }

    @Override
    public void close() {
    }

}
