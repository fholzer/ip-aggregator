package com.gvcgroup.ipaggregator.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gvcgroup.ipaggregator.definitions.AccessLog;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ferdinand Holzer
 */
public class ActualClientIpValueMapper implements ValueMapper<ObjectNode, ObjectNode> {
    public static final String FIELDNAME_ACTUALREMOTEADDR = "actualRemoteAddr";
    private static final Logger log = LoggerFactory.getLogger(ActualClientIpValueMapper.class);
    private final Charset UTF8;
    private final String cloudflareIpV4List = "https://www.cloudflare.com/ips-v4";
    private final String cloudflareIpV6List = "https://www.cloudflare.com/ips-v6";
    private final List<SubnetUtils.SubnetInfo> privateNetworks;
    private Set<SubnetUtils.SubnetInfo> trustedNetworks = null;

    public ActualClientIpValueMapper() {
        UTF8 = Charset.forName("UTF-8");
        privateNetworks = buildPrivateNetworks();
        try {
            updateTrustedNetworkList();
        } catch(IOException ex) {
            log.error("Failed to load trusted networks");
        }
    }

    private List<SubnetUtils.SubnetInfo> buildPrivateNetworks() {
        String[] nets = new String[] {
            "10.0.0.0/8",
            "172.16.0.0/12",
            "192.0.0.0/24",
            "192.168.0.0/16",
            "198.18.0.0/15" /*,
            "fc00::/7" */
        };

        return Arrays.asList(nets)
                .stream()
                .map((r) -> new SubnetUtils(r).getInfo())
                .collect(Collectors.toList());
    }

    // this is only meant to be used with small files!
    private List<SubnetUtils.SubnetInfo> buildFromURL(URL url) throws IOException {
        ArrayList<SubnetUtils.SubnetInfo> res = new ArrayList<>();
        try(BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream(), UTF8))) {
            String cidr;
            while((cidr = br.readLine()) != null) {
                res.add(new SubnetUtils(cidr).getInfo());
            }
        }
        return res;
    }

    private void updateTrustedNetworkList() throws IOException {
        Set<SubnetUtils.SubnetInfo> res = new HashSet<>();

        // private subnets
        res.addAll(privateNetworks);

        // cloudflare
        res.addAll(this.buildFromURL(new URL(cloudflareIpV4List)));
        //res.addAll(this.buildFromURL(new URL(cloudflareIpV6List)));

        this.trustedNetworks = res;
    }

    private boolean isTrusted(String ip) {
        if(this.trustedNetworks.stream().anyMatch((si) -> (si.isInRange(ip)))) {
            return true;
        }
        return false;
    }

    @Override
    public ObjectNode apply(ObjectNode v) {
        ObjectNode res = v.deepCopy();
        JsonNode n;
        String remoteAddr = null;
        String xFwdFor[] = null;

        n = res.get(AccessLog.REMOTEADDR);
        if(n != null && n.isTextual()) {
            remoteAddr = n.asText();
        }

        n = res.get(AccessLog.XFWDFOR);
        if(n != null && n.isTextual()) {
            String xFwdForRaw[] = n.asText().split(",");
            List<String> xFwdForList = Arrays.asList(xFwdForRaw).stream()
                    .map((s) -> s.replace('+', ' ').trim())
                    .filter((s) -> s.length() > 0)
                    .collect(Collectors.toList());
            xFwdFor = xFwdForList.toArray(new String[xFwdForList.size()]);
        }

        if(remoteAddr != null && xFwdFor != null) {
            for(int i = xFwdFor.length -1; i >= 0 && isTrusted(remoteAddr); i--) {
                remoteAddr = xFwdFor[i];
            }
        }

        res.put(FIELDNAME_ACTUALREMOTEADDR, remoteAddr);
        return res;
    }

    public List<String> getTrustedNetworks() {
        return this.trustedNetworks.stream().map((si) -> si.getCidrSignature()).collect(Collectors.toList());
    }

}
