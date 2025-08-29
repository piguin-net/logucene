package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Settings {

    private static Logger logger = LoggerFactory.getLogger(Settings.class);

    public static int getSyslogPort() {
        return Integer.valueOf(System.getProperty(
            "syslog.port",
            System.getenv().containsKey("SYSLOG_PORT")
                ? System.getenv("SYSLOG_PORT")
                : "514"
        ));
    }

    public static int getWebPort() {
        return Integer.valueOf(System.getProperty(
            "web.port",
            System.getenv().containsKey("WEB_PORT")
                ? System.getenv("WEB_PORT")
                : "8080"
        ));
    }

    public static String getLuceneIndex() {
        return System.getProperty(
            "lucene.index",
            System.getenv().containsKey("LUCENE_INDEX")
                ? System.getenv("LUCENE_INDEX")
                : "index"
        );
    }

    public static String getLuceneAnalyzer() {
        return System.getProperty(
            "lucene.analyzer",
            System.getenv().containsKey("LUCENE_ANALYZER")
                ? System.getenv("LUCENE_ANALYZER")
                : "org.apache.lucene.analysis.standard.StandardAnalyzer"
        );
    }

    public static String getSqliteAnalyzer() {
        return System.getProperty(
            "sqlite.analyzer",
            System.getenv().containsKey("SQLITE_ANALYZER")
                ? System.getenv("SQLITE_ANALYZER")
                : "org.apache.lucene.analysis.standard.StandardAnalyzer"
        );
    }

    public static String getUserTimezone() {
        return System.getProperty(
            "system.timezone",
            System.getenv().containsKey("SYSTEM_TIMEZONE")
                ? System.getenv("SYSTEM_TIMEZONE")
                : System.getProperty("user.timezone")
        );
    }

    public static String getSyslogTimezone() {
        return System.getProperty(
            "syslog.timezone",
            System.getenv().containsKey("SYSLOG_TIMEZONE")
                ? System.getenv("SYSLOG_TIMEZONE")
                : getUserTimezone()
        );
    }

    public static String getSyslogTimezone(String addr) {
        String _addr = addr.replace(".", "_").replace(":", "_");
        return System.getProperty(
            "syslog.timezone["+addr+"]",
            System.getenv().containsKey("SYSLOG_TIMEZONE_"+_addr)
                ? System.getenv("SYSLOG_TIMEZONE_"+_addr)
                : getSyslogTimezone()
        );
    }

    public static String getSyslogListener() {
        return System.getProperty(
            "syslog.listener",
            System.getenv().containsKey("SYSLOG_LISTENER")
                ? System.getenv("SYSLOG_LISTENER")
                : null
        );
    }

    public static void print() {
        logger.info("Settings:");
        logger.info("  syslog.port=" + getSyslogPort());
        logger.info("  web.port=" + getWebPort());
        logger.info("  lucene.index=" + getLuceneIndex());
        logger.info("  lucene.analyzer=" + getLuceneAnalyzer());
        logger.info("  sqlite.analyzer=" + getSqliteAnalyzer());
        logger.info("  system.timezone=" + getUserTimezone());
        logger.info("  syslog.timezone=" + getSyslogTimezone());
        logger.info("  syslog.listener=" + getSyslogListener());
    }
}
