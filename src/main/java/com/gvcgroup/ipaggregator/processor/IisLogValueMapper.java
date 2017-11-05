package com.gvcgroup.ipaggregator.processor;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gvcgroup.ipaggregator.definitions.AccessLog;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ferdinand Holzer
 */
public class IisLogValueMapper extends AbstractLogValueMapper {
    private static final Logger log = LoggerFactory.getLogger(IisLogValueMapper.class);
    private static final String ERROR_PARSE_TS = "Failed to parse record's timestamp.";
    public static final String FIELDNAME_IP = "localIp";
    public static final String FIELDNAME_PORT = "localPort";
    public static final String FIELDNAME_USERNAME = "username";
    public static final String FIELDNAME_SUBSTATUS = "subStatus";
    public static final String FIELDNAME_WIN32STATUS = "win32Status";
    public static final String TAG_EXTRA_FIELDS = "extrafields";
    public static final String EMPTY = "-";
    public static final String PATTERN = "^(?<ts>[^ ]+ [^ ]+) (?<ip>[0-9.]+) (?<method>[^ ]+) " +
                "(?<uri>[^ ]+) (?<args>[^ ]+) (?<port>[0-9]+) " +
                "(?<username>[^ ]+) (?<remoteaddr>[0-9.]+) (?<useragent>[^ ]+) " +
                "(?<referer>[^ ]+) (?<status>[0-9]+) (?<substatus>[0-9]+) " +
                "(?<win32status>[0-9]+) (?<responsetime>[0-9]+) " +
                "(?<xfwdfor>[^ ]+)(?<extra> .*)?$";
    private final Pattern pattern;

    public IisLogValueMapper() {
        this.pattern = Pattern.compile(PATTERN, Pattern.DOTALL);
    }

    @Override
    public ObjectNode apply(ObjectNode v) {
        ObjectNode res = v.deepCopy();
        String msg = res.get("message").asText();

        Matcher m = pattern.matcher(msg);
        if(!m.matches()) {
            this.addTag(res, AbstractLogValueMapper.TAG_PARSE_ERROR);
            return res;
        }
        res.remove("message");

        try {
            Calendar cDate = javax.xml.bind.DatatypeConverter.parseDateTime(m.group("ts").replace(' ', 'T'));
            res.put(AccessLog.FIELDNAME_TS, cDate.getTimeInMillis());
        } catch(Exception ex) {
            log.error(ERROR_PARSE_TS, ex);
        }

        res.put(FIELDNAME_IP, m.group("ip"));
        res.put(FIELDNAME_PORT, m.group("port"));
        res.put(AccessLog.FIELDNAME_METHOD, m.group("method"));
        res.put(AccessLog.FIELDNAME_URI, m.group("uri"));
        res.put(AccessLog.FIELDNAME_REMOTEADDR, m.group("remoteaddr"));
        res.put(AccessLog.FIELDNAME_STATUS, m.group("status"));
        res.put(FIELDNAME_SUBSTATUS, m.group("substatus"));
        res.put(FIELDNAME_WIN32STATUS, m.group("win32status"));
        res.put(AccessLog.FIELDNAME_RESPONSETIME, m.group("responsetime"));

        String tmp;
        tmp = m.group("args");
        if(!tmp.equals(EMPTY)) {
            res.put(AccessLog.FIELDNAME_ARGS, tmp);
        }

        tmp = m.group("username");
        if(!tmp.equals(EMPTY)) {
            res.put(FIELDNAME_USERNAME, tmp);
        }

        tmp = m.group("useragent");
        if(!tmp.equals(EMPTY)) {
            res.put(AccessLog.FIELDNAME_USERAGENT, tmp.replace('+', ' '));
        }

        tmp = m.group("referer");
        if(!tmp.equals(EMPTY)) {
            res.put(AccessLog.FIELDNAME_REFERER, tmp);
        }

        tmp = m.group("xfwdfor");
        if(!tmp.equals(EMPTY)) {
            res.put(AccessLog.FIELDNAME_XFWDFOR, tmp.replace('+', ' '));
        }

        tmp = m.group("extra");
        if(tmp != null) {
            res.put(AccessLog.FIELDNAME_EXTRA, tmp.substring(1));
            this.addTag(res, TAG_EXTRA_FIELDS);
        }

        return res;
    }

}
