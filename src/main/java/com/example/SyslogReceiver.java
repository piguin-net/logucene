package com.example;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

import com.example.LuceneManager.LuceneFieldKeys;

public class SyslogReceiver implements Runnable {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public static enum Facility {
        unknown(-1),
        kern(0),
        user(1),
        mail(2),
        daemon(3),
        auth(4),
        syslog(5),
        lpr(6),
        news(7),
        uucp(8),
        cron(9),
        authpriv(10),
        ftp(11),
        ntp(12),
        logAudit(13),
        logAlert(14),
        clock(15),
        local0(16),
        local1(17),
        local2(18),
        local3(19),
        local4(20),
        local5(21),
        local6(22),
        local7(23);

        private int key;

        Facility(int key) {
            this.key = key;
        }

        public static Facility of(int key) {
            for (Facility item: Facility.values()) {
                if (item.key == key) {
                    return item;
                }
            }
            return unknown;
        }
    }

    public static enum Severity {
        unknown(-1),
        emerg(0),
        alert(1),
        crit(2),
        err(3),
        warning(4),
        notice(5),
        info(6),
        debug(7);

        private int key;

        Severity(int key) {
            this.key = key;
        }

        public static Severity of(int key) {
            for (Severity item: Severity.values()) {
                if (item.key == key) {
                    return item;
                }
            }
            return unknown;
        }
    };

    private final int port;
    private final LuceneManager lucene;
    private final DatagramSocket socket;
    private boolean active = true;
    private List<Consumer<Document>> onReceive = new ArrayList<>();

    public SyslogReceiver(int port, LuceneManager lucene) throws SocketException {
        this.port = port;
        this.lucene = lucene;
        this.socket = new DatagramSocket(this.port);
    }

    public void stop() {
        this.active = false;
        this.socket.close();
    }

    public void addEventListener(Consumer<Document> onReceive) {
        this.onReceive.add(onReceive);
    }

    @Override
    public void run() {
        byte[] buf = new byte[65535];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        try {
            logger.atInfo().setMessage(
                "SyslogReceiver start."
            ).addKeyValue(
                "syslog port", this.port
            ).addKeyValue(
                "lucene index dir", lucene.getDirectory()
            ).log();
            while (this.active) {
                try {
                    socket.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength());
                    Function<LoggingEventBuilder, LoggingEventBuilder> log = (builder) -> {
                        return builder.setMessage(
                            message
                        ).addKeyValue(
                            LuceneFieldKeys.addr.name(), packet.getAddress().getHostAddress()
                        ).addKeyValue(
                            LuceneFieldKeys.port.name(), packet.getPort()
                        );
                    };
                    try {
                        Document doc = this.parse(
                            packet.getAddress().getHostAddress(),
                            packet.getPort(),
                            message
                        );
                        lucene.add(doc);
                        for (Consumer<Document> listener: this.onReceive) {
                            listener.accept(doc);
                        }
                        log.apply(logger.atDebug()).log();
                    } catch (IOException e) {
                        log.apply(logger.atError()).log();
                    }
                } catch (SocketException e) {
                    if (this.active) {
                        if (this.socket.isClosed()) {
                            throw e;
                        } else {
                            logger.atError().log("SyslogReceiver receive failed.", e);
                        }
                    }
                }
            }
            logger.atInfo().log("SyslogReceiver stop.");
        } catch (Exception e) {
            logger.atError().log("SyslogReceiver stop failed.", e);
        }
        if (!this.socket.isClosed()) this.socket.close();
    }

    private Document parse(String host, int port, String message) {
        Document doc = new Document();
        doc.add(new KeywordField(
            LuceneFieldKeys.addr.name(),
            host,
            Field.Store.YES
        ));
        doc.add(new IntField(
            LuceneFieldKeys.port.name(),
            port,
            Field.Store.YES
        ));
        Integer priority = this.getPriority(message);
        if (priority != null) {
            doc.add(new KeywordField(
                LuceneFieldKeys.facility.name(),
                Facility.of(priority / 8).name(),
                Field.Store.YES
            ));
            doc.add(new KeywordField(
                LuceneFieldKeys.severity.name(),
                Severity.of(priority % 8).name(),
                Field.Store.YES
            ));
        }
        ZonedDateTime date = ZonedDateTime.now(
            ZoneId.of(System.getProperty("user.timezone"))
        );
        doc.add(new LongField(
            LuceneFieldKeys.timestamp.name(),
            date.toInstant().toEpochMilli(),
            Field.Store.YES
        ));
        doc.add(new KeywordField(
            LuceneFieldKeys.day.name(),
            date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
            Field.Store.YES
        ));
        doc.add(new KeywordField(
            LuceneFieldKeys.time.name(),
            date.format(DateTimeFormatter.ofPattern("HH:mm:ss")),
            Field.Store.YES
        ));
        doc.add(new TextField(
            LuceneFieldKeys.message.name(),
            message,
            Field.Store.YES
        ));
        return doc;
    }

    private Integer getPriority(String message) {
        boolean priority = false;
        String buf = "";
        for (char c: message.toCharArray()) {
            if (priority) {
                if (c == '>') {
                    break;
                } else if ('0' <= c && c <= '9') {
                    buf = buf + c;
                } else {
                    return null;
                }
            } else {
                if (c == '<') {
                    priority = true;
                }
            }
        }
        return "".equals(buf) ? null : Integer.valueOf(buf);
    }
    
}
