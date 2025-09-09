package com.example;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

import com.example.LuceneManager.LuceneReader;
import com.example.SyslogParser.Facility;
import com.example.SyslogParser.Rfc3164;
import com.example.SyslogParser.Severity;
import com.example.SyslogParser.SyslogParseException;

import me.tongfei.progressbar.ProgressBar;

public class SyslogReceiver implements Runnable {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public static enum Time {
        SECOND(1000),
        MINUTE(Time.SECOND.time * 60),
        HOUR(Time.MINUTE.time * 60),
        DAY(Time.HOUR.time * 24);

        private final int time;

        private Time(int time) {
            this.time = time;
        }

        public int getTime() {
            return this.time;
        }
    }

    public static enum LuceneFieldKeys {
        sort(LongField.class, long.class),
        timestamp(LongPoint.class, long[].class),
        host(StringField.class, String.class),
        addr(StringField.class, String.class),
        port(IntPoint.class, int[].class),
        facility(StringField.class, String.class),
        severity(StringField.class, String.class),
        format(StringField.class, String.class),
        message(TextField.class, String.class),
        raw(TextField.class, String.class);

        private Class<? extends Field> fieldClazz;
        private Class<?> valueClazz;

        <T extends Field> LuceneFieldKeys(Class<T> fieldClazz, Class<?> valueClazz) {
            this.fieldClazz = fieldClazz;
            this.valueClazz = valueClazz;
        }

        public Class<? extends Field> getFieldClass() {
            return this.fieldClazz;
        }

        public <T> Field field(T value) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
            if (this.fieldClazz.equals(IntPoint.class)) {
                return this.fieldClazz.getDeclaredConstructor(
                    String.class,
                    this.valueClazz
                ).newInstance(
                    this.name(),
                    new int[]{(int) value}
                );
            } else if (this.fieldClazz.equals(LongPoint.class)) {
                return this.fieldClazz.getDeclaredConstructor(
                    String.class,
                    this.valueClazz
                ).newInstance(
                    this.name(),
                    new long[]{(long) value}
                );
            } else {
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
        }

        public static Map<String, PointsConfig> getPointsConfig(ZoneOffset offset) {
            return new HashMap<>() {{
                DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
                this.put(LuceneFieldKeys.port.name(), new PointsConfig(new DecimalFormat(), Integer.class));
                this.put(LuceneFieldKeys.timestamp.name(), new PointsConfig(new DecimalFormat() {
                    @Override
                    public Number parse(String text) throws java.text.ParseException {
                        for (String value: Arrays.asList(text, text + ".000", text + ":00.000", text + " 00:00:00.000")) {
                            try {
                                return LocalDateTime.parse(value, format).atOffset(offset).toInstant().toEpochMilli();
                            } catch (Exception e) {
                                // pass
                            }
                        }
                        throw new java.text.ParseException("illegal date format.", -1);
                    }
                }, Long.class));
            }};
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
                        // TODO; バッファリング
                        Document doc = SyslogReceiver.parse(
                            new Date().getTime(),
                            packet.getAddress().getHostAddress(),
                            packet.getPort(),
                            message
                        );
                        for (Consumer<Document> listener: this.onReceive) {
                            listener.accept(doc);
                        }
                        lucene.add(doc);
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

    public static Document parse(long timestamp, String addr, int port, String message) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        Supplier<Document> init = () -> {
            Document doc = new Document();
            try {
                doc.add(LuceneFieldKeys.raw.field(message));
                doc.add(LuceneFieldKeys.sort.field(timestamp));
                doc.add(LuceneFieldKeys.timestamp.field(timestamp));
                doc.add(LuceneFieldKeys.addr.field(addr));
                doc.add(LuceneFieldKeys.port.field(port));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return doc;
        };
        Document doc = init.get();
        try {
            Rfc3164 log = SyslogParser.parse(message);
            doc.add(LuceneFieldKeys.facility.field(log.facility.name()));
            doc.add(LuceneFieldKeys.severity.field(log.severity.name()));
            doc.add(LuceneFieldKeys.host.field(log.host));
            doc.add(LuceneFieldKeys.message.field(log.message));
            doc.add(LuceneFieldKeys.format.field(log.format));
            return doc;
        } catch (SyslogParseException e) {
            Integer priority = SyslogParser.parsePriority(message).getKey();
            if (priority != null) {
                doc.add(LuceneFieldKeys.facility.field(Facility.of(priority).name()));
                doc.add(LuceneFieldKeys.severity.field(Severity.of(priority).name()));
            }
            doc.add(LuceneFieldKeys.host.field(addr));
            doc.add(LuceneFieldKeys.message.field(message));
            doc.add(LuceneFieldKeys.format.field("unknown"));
            return doc;
        } finally {
            // TODO: 処理を共通化
            doc.add(new SortedNumericDocValuesField(LuceneFieldKeys.sort.name(), timestamp));
            doc.add(new StoredField(LuceneFieldKeys.timestamp.name(), timestamp));
            doc.add(new SortedDocValuesField(LuceneFieldKeys.timestamp.name(), new BytesRef(ByteBuffer.allocate(8).putLong(timestamp).array())));
            doc.add(new StoredField(LuceneFieldKeys.port.name(), port));
            doc.add(new SortedDocValuesField(LuceneFieldKeys.port.name(), new BytesRef(ByteBuffer.allocate(4).putInt(port).array())));
            for (LuceneFieldKeys field: Arrays.asList(LuceneFieldKeys.values()).stream().filter(field -> field.fieldClazz == StringField.class).toList()) {
                doc.add(new SortedDocValuesField(field.name(), new BytesRef(doc.get(field.name()))));
            }
        }
    }

    public static Map<String, String> toMap(Document doc, ZoneOffset offset) {
        long timestamp = Long.valueOf(doc.get(LuceneFieldKeys.timestamp.name()));
        OffsetDateTime datetime = OffsetDateTime.ofInstant(new Date(timestamp).toInstant(), offset);
        return new HashMap<>() {{
            this.put("day", datetime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
            this.put("time", datetime.format(DateTimeFormatter.ofPattern("HH:mm:ss")));
            for (LuceneFieldKeys field: LuceneFieldKeys.values()) {
                this.put(field.name(), doc.get(field.name()));
            }
        }};
    }

    public static void main(String[] args) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException, IOException, ParseException, QueryNodeException  {
        try (
            LuceneManager src = new LuceneManager(System.getProperty("lucene.migration.src", "index"));
            LuceneManager dst = new LuceneManager(System.getProperty("lucene.migration.dst", "migrated"));
            LuceneReader reader = src.getReader();
        ) {
            TopDocs hits = reader.search(
                LuceneFieldKeys.message.name(),
                "*:*",
                new Sort(new SortedNumericSortField(
                    LuceneFieldKeys.sort.name(),
                    SortField.Type.LONG,
                    true
                )),
                LuceneFieldKeys.getPointsConfig(ZoneOffset.UTC)
            );
            int chunk = Integer.getInteger("lucene.migration.chunk", 1000);
            List<Document> docs = new ArrayList<>();
            for (ScoreDoc score: ProgressBar.wrap(Arrays.asList(hits.scoreDocs), "migration")) {
                Document srcDoc = reader.get(score.doc);
                long timestamp = Long.valueOf(srcDoc.get(LuceneFieldKeys.timestamp.name()));
                Document dstDoc = SyslogReceiver.parse(
                    timestamp,
                    srcDoc.get(LuceneFieldKeys.addr.name()),
                    Integer.valueOf(srcDoc.get(LuceneFieldKeys.port.name())),
                    srcDoc.get(LuceneFieldKeys.raw.name())
                );
                docs.add(dstDoc);
                if (docs.size() == chunk) {
                    dst.add(docs);
                    docs.clear();
                }
            }
            if (docs.size() > 0) {
                dst.add(docs);
            }
        }
    }
    
}
