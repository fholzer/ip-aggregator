package com.gvcgroup.ipaggregator.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gvcgroup.ipaggregator.definitions.AccessLog;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Ferdinand Holzer
 */
public class ActualClientIpValueMapperTest {
    private final ObjectMapper mapper;
    private final ActualClientIpValueMapper instance;

    public ActualClientIpValueMapperTest() {
        mapper = new ObjectMapper();
        instance = new ActualClientIpValueMapper();

        System.out.println("Trusted Network List:");
        instance.getTrustedNetworks().forEach((cidr) -> System.out.println(cidr));
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of apply method, of class ActualClientIpValueMapper.
     */

    private void testApplySingle(String remoteAddr, String xFwdFor, String expResult) {
        ObjectNode v = mapper.createObjectNode();
        v.put(AccessLog.REMOTEADDR, remoteAddr);
        v.put(AccessLog.XFWDFOR, xFwdFor);
        ObjectNode result = instance.apply(v);
        assertEquals(expResult, result.get(ActualClientIpValueMapper.FIELDNAME_ACTUALREMOTEADDR).asText());
    }

    @Test
    public void testApply() {
        System.out.println("apply");
        testApplySingle("162.158.90.142", "81.173.164.254,+198.41.242.95", "81.173.164.254");
        testApplySingle("172.68.226.62", "213.139.54.39, 162.158.98.74", "213.139.54.39");
        testApplySingle("198.41.238.56", "51.171.80.91, 162.158.38.26", "51.171.80.91");
    }

}
