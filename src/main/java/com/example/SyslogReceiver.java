package com.example;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.text.DecimalFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

import com.example.SyslogParser.Rfc3164;
import com.example.SyslogParser.Rfc5424;
import com.example.SyslogParser.SyslogParseException;

public class SyslogReceiver implements Runnable {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public static enum LuceneFieldKeys {
        timestamp(LongField.class, long.class, new PointsConfig(new DecimalFormat(), Long.class)),
        day(KeywordField.class, String.class),
        time(KeywordField.class, String.class),
        host(TextField.class, String.class),
        addr(KeywordField.class, String.class),
        port(IntField.class, int.class, new PointsConfig(new DecimalFormat(), Integer.class)),
        facility(KeywordField.class, String.class),
        severity(KeywordField.class, String.class),
        format(KeywordField.class, String.class),
        message(TextField.class, String.class),
        raw(TextField.class, String.class);

        private Class<? extends Field> fieldClazz;
        private Class<?> valueClazz;
        private PointsConfig pointsConfig;

        <T extends Field> LuceneFieldKeys(Class<T> fieldClazz, Class<?> valueClazz) {
            this(fieldClazz, valueClazz, null);
        }

        <T extends Field> LuceneFieldKeys(Class<T> fieldClazz, Class<?> valueClazz, PointsConfig pointsConfig) {
            this.fieldClazz = fieldClazz;
            this.valueClazz = valueClazz;
            this.pointsConfig = pointsConfig;
        }

        public <T> Field field(T value) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
            return this.fieldClazz.getDeclaredConstructor(
                String.class,
                this.valueClazz,
                Field.Store.class
            ).newInstance(
                this.name(),
                value,
                Field.Store.YES
            );
        }

        public static Map<String, PointsConfig> getPointsConfig() {
            return new HashMap<>() {{
                for (LuceneFieldKeys field: LuceneFieldKeys.values()) {
                    if (field.pointsConfig != null) {
                        this.put(field.name(), field.pointsConfig);
                    }
                }
            }};
        }
    };

    public static enum Facility {
        unknown(null),
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

        private Integer key;

        Facility(Integer key) {
            this.key = key;
        }

        public static Facility of(Integer key) {
            for (Facility item: Facility.values()) {
                if (item.key == key) {
                    return item;
                }
            }
            return unknown;
        }
    }

    public static enum Severity {
        unknown(null),
        emerg(0),
        alert(1),
        crit(2),
        err(3),
        warning(4),
        notice(5),
        info(6),
        debug(7);

        private Integer key;

        Severity(Integer key) {
            this.key = key;
        }

        public static Severity of(Integer key) {
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

    private Document parse(String addr, int port, String message) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        Document doc = new Document();
        ZoneId userTimezone = ZoneId.of(System.getProperty("user.timezone"));
        ZonedDateTime now = ZonedDateTime.now();
        doc.add(LuceneFieldKeys.raw.field(message));
        doc.add(LuceneFieldKeys.timestamp.field(now.toInstant().toEpochMilli()));
        doc.add(LuceneFieldKeys.addr.field(addr));
        doc.add(LuceneFieldKeys.port.field(port));
        try {
            Rfc3164 log = SyslogParser.parse(message);
            doc.add(LuceneFieldKeys.facility.field(Facility.of(log.facility).name()));
            doc.add(LuceneFieldKeys.severity.field(Severity.of(log.severity).name()));
            doc.add(LuceneFieldKeys.host.field(log.host));
            doc.add(LuceneFieldKeys.message.field(log.message));
            ZoneId syslogTimezone;
            if (log instanceof Rfc5424) {
                syslogTimezone = ((Rfc5424) log).zone;
            } else {
                syslogTimezone = ZoneId.of(
                    System.getProperty("syslog.timezone[" + addr + "]",
                    System.getProperty("syslog.timezone",
                    System.getProperty("user.timezone")
                )));
            }
            ZonedDateTime date = log.date.atZone(syslogTimezone);
            date = date.withZoneSameInstant(userTimezone);
            doc.add(LuceneFieldKeys.day.field(date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))));
            doc.add(LuceneFieldKeys.time.field(date.format(DateTimeFormatter.ofPattern("HH:mm:ss"))));
            doc.add(LuceneFieldKeys.format.field(log.format));
        } catch (SyslogParseException e) {
            Integer priority = SyslogParser.parsePriority(message).getKey();
            if (priority != null) {
                doc.add(LuceneFieldKeys.facility.field(Facility.of(priority / 8).name()));
                doc.add(LuceneFieldKeys.severity.field(Severity.of(priority % 8).name()));
            }
            doc.add(LuceneFieldKeys.host.field(addr));
            doc.add(LuceneFieldKeys.day.field(now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))));
            doc.add(LuceneFieldKeys.time.field(now.format(DateTimeFormatter.ofPattern("HH:mm:ss"))));
            doc.add(LuceneFieldKeys.message.field(message));
            doc.add(LuceneFieldKeys.format.field("unknown"));
        }
        return doc;
    }
    
}
