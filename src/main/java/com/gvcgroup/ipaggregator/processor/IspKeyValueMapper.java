package com.gvcgroup.ipaggregator.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gvcgroup.ipaggregator.model.Isp;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.IspResponse;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ferdinand Holzer
 */
public class IspKeyValueMapper implements KeyValueMapper<Object, ObjectNode, Isp> {
    private static final Logger log = LoggerFactory.getLogger(IspKeyValueMapper.class);
    private static final String ERR_FAILED_EXTRACT = "Failed to extract field from object.";
    private static final String ERR_IP_NOT_FOUND = "IP Address not found.";
    private static final String ERR_GENERIC = "Error evaluating ISP information";
    private final DatabaseReader dr;
    private final String fieldname;

    public IspKeyValueMapper(String fieldname) throws IOException {
        this.dr = new DatabaseReader.Builder(new File("GeoIP2-ISP.mmdb")).withCache(new CHMCache()).build();
        this.fieldname = fieldname;
    }

    @Override
    public Isp apply(Object k, ObjectNode v) {
        try {
            JsonNode n = v.get(fieldname);
            if(n == null || !n.isTextual()) {
                throw new RuntimeException(ERR_FAILED_EXTRACT);
            }
            InetAddress ip = InetAddress.getByName(n.asText());
            IspResponse ir = dr.isp(ip);
            return new Isp(ir.getAutonomousSystemNumber(), ir.getAutonomousSystemOrganization(), ir.getIsp(), ir.getOrganization());
        } catch(AddressNotFoundException ex) {
            log.info(ERR_IP_NOT_FOUND);
        } catch(Exception ex) {
            log.error(ERR_GENERIC, ex);
        }
        return null;
    }

}
