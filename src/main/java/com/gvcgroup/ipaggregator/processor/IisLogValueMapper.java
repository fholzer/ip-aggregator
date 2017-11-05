package com.gvcgroup.ipaggregator.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ferdinand Holzer
 */
public class IisLogValueMapper implements ValueMapper<ObjectNode, ObjectNode> {
    private static final Logger log = LoggerFactory.getLogger(IisLogValueMapper.class);
    private static final String ERROR_PARSE_TS = "Failed to parse record's timestamp.";
    public static final String FIELDNAME_TS = "@timestamp";
    public static final String FIELDNAME_IP = "localIp";
    public static final String FIELDNAME_METHOD = "httpVerb";
    public static final String FIELDNAME_URI = "request";
    public static final String FIELDNAME_ARGS = "args";
    public static final String FIELDNAME_PORT = "localPort";
    public static final String FIELDNAME_USERNAME = "username";
    public static final String FIELDNAME_REMOTEADDR = "remoteAddr";
    public static final String FIELDNAME_USERAGENT = "userAgent";
    public static final String FIELDNAME_REFERER = "referer";
    public static final String FIELDNAME_STATUS = "status";
    public static final String FIELDNAME_SUBSTATUS = "subStatus";
    public static final String FIELDNAME_WIN32STATUS = "win32Status";
    public static final String FIELDNAME_RESPONSETIME = "responseTime";
    public static final String FIELDNAME_XFWDFOR = "xForwardedFor";
    public static final String FIELDNAME_EXTRA = "extraneous";
    public static final String FIELDNAME_TAG = "tag";
    public static final String TAG_PARSE_ERROR = "parseerror";
    public static final String TAG_EXTRA_FIELDS = "extrafields";
    public static final String EMPTY = "-";
    private final Pattern pattern;

    public IisLogValueMapper() {
        this.pattern = Pattern.compile(
                "^(?<ts>[^ ]+ [^ ]+) (?<ip>[0-9.]+) (?<method>[^ ]+) " +
                "(?<uri>[^ ]+) (?<args>[^ ]+) (?<port>[0-9]+) " +
                "(?<username>[^ ]+) (?<remoteaddr>[0-9.]+) (?<useragent>[^ ]+) " +
                "(?<referer>[^ ]+) (?<status>[0-9]+) (?<substatus>[0-9]+) " +
                "(?<win32status>[0-9]+) (?<responsetime>[0-9]+) " +
                "(?<xfwdfor>[^ ]+)(?<extra> .*)?$", Pattern.DOTALL);
    }

    private void addTag(ObjectNode parent, String tagName) {
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

    @Override
    public ObjectNode apply(ObjectNode v) {
        ObjectNode res = v.deepCopy();
        String msg = v.get("message").asText();
        Matcher m = pattern.matcher(msg);
        if(!m.matches()) {
            this.addTag(res, TAG_PARSE_ERROR);
            return res;
        }

        try {
            Calendar cDate = javax.xml.bind.DatatypeConverter.parseDateTime(m.group("ts").replace(' ', 'T'));
            res.put(FIELDNAME_TS, cDate.getTimeInMillis());
        } catch(Exception ex) {
            log.error(ERROR_PARSE_TS, ex);
        }
        
        res.put(FIELDNAME_IP, m.group("ip"));
        res.put(FIELDNAME_PORT, m.group("port"));
        res.put(FIELDNAME_METHOD, m.group("method"));
        res.put(FIELDNAME_URI, m.group("uri"));
        res.put(FIELDNAME_REMOTEADDR, m.group("remoteaddr"));
        res.put(FIELDNAME_STATUS, m.group("status"));
        res.put(FIELDNAME_SUBSTATUS, m.group("substatus"));
        res.put(FIELDNAME_WIN32STATUS, m.group("win32status"));
        res.put(FIELDNAME_RESPONSETIME, m.group("responsetime"));

        String tmp;
        tmp = m.group("args");
        if(!tmp.equals(EMPTY)) {
            res.put(FIELDNAME_ARGS, tmp);
        }

        tmp = m.group("username");
        if(!tmp.equals(EMPTY)) {
            res.put(FIELDNAME_USERNAME, tmp);
        }

        tmp = m.group("useragent");
        if(!tmp.equals(EMPTY)) {
            res.put(FIELDNAME_USERAGENT, tmp.replace('+', ' '));
        }

        tmp = m.group("referer");
        if(!tmp.equals(EMPTY)) {
            res.put(FIELDNAME_REFERER, tmp);
        }

        tmp = m.group("xfwdfor");
        if(!tmp.equals(EMPTY)) {
            res.put(FIELDNAME_XFWDFOR, tmp.replace('+', ' '));
        }

        tmp = m.group("extra");
        if(tmp != null) {
            res.put(FIELDNAME_EXTRA, tmp.substring(1));
            this.addTag(res, TAG_EXTRA_FIELDS);
        }

        return res;
    }

}
