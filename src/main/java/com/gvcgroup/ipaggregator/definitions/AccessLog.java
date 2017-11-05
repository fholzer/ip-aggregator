package com.gvcgroup.ipaggregator.definitions;

/**
 *
 * @author Ferdinand Holzer
 */
public class AccessLog {
    public static final String FIELDNAME_TS = "@timestamp";
    public static final String FIELDNAME_METHOD = "httpVerb";
    public static final String FIELDNAME_URI = "request";
    public static final String FIELDNAME_ARGS = "args";
    public static final String FIELDNAME_USERAGENT = "userAgent";
    public static final String FIELDNAME_REFERER = "referer";
    public static final String FIELDNAME_STATUS = "status";
    public static final String FIELDNAME_RESPONSETIME = "responseTime";
    public static final String FIELDNAME_EXTRA = "extraneous";
    public static final String FIELDNAME_REMOTEADDR = "remoteAddr";
    public static final String FIELDNAME_XFWDFOR = "xForwardedFor";
}
