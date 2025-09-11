package com.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.DayOfWeek;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.grouping.LongRange;
import org.apache.lucene.search.grouping.LongRangeFactory;
import org.apache.lucene.search.grouping.LongRangeGroupSelector;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.Job.Progress;
import com.example.LuceneManager.LuceneReader;
import com.example.SyslogParser.Facility;
import com.example.SyslogParser.Severity;
import com.example.SyslogReceiver.LuceneFieldKeys;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import io.javalin.http.UploadedFile;
import io.javalin.http.staticfiles.Location;
import io.javalin.websocket.WsConnectContext;

/**
 * Hello world!
 */
public class Main 
{
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    private static ObjectMapper mapper = new ObjectMapper();
    private static LuceneManager lucene;
    private static SyslogReceiver watcher;
    private static Thread worker;
    private static Map<Integer, WsConnectContext> realtimeConnections = new HashMap<>();
    private static Map<Integer, WsConnectContext> jobConnections = new HashMap<>();
    private static String script = Settings.getSyslogListener();
    private static ScriptEngineManager manager = new ScriptEngineManager();
    private static ScriptEngine engine = manager.getEngineByName("groovy");
    private static Map<Integer, ImportExportJob> jobs = new HashMap<>();

    public static enum JobType {
        Export,
        Import
    }
    public static enum FileFormat {
        SQLite("sqlite3"),
        TSV("tsv");
        private final String ext;
        private FileFormat(String ext) {
            this.ext = ext;
        }
        public String getExt() {
            return this.ext;
        }
    }
    public static class ImportExportJob extends Job<TempFile> {
        public ImportExportJob(TempFile data, Task<TempFile> task) {
            super(data, task);
        }

        private JobType type;
        private FileFormat format;

        public JobType getType() {
            return type;
        }
        public void setType(JobType type) {
            this.type = type;
        }

        public FileFormat getFormat() {
            return format;
        }
        public void setFormat(FileFormat format) {
            this.format = format;
        }

        public Map<String, Object> payload(ZoneOffset offset) {
            Function<Long, String> convert = (timestamp) -> {
                if (timestamp != null) {
                    return OffsetDateTime
                        .ofInstant(new Date(timestamp).toInstant(), offset)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                } else {
                    return null;
                }
            };
            Map<String, Object> result = new HashMap<>();
            result.put("type", this.getType().name().toLowerCase());
            result.put("event", this.getProgress().event.name());
            result.put("id", this.hashCode());
            result.put("start", convert.apply(this.getStartTime()));
            result.put("finish", convert.apply(this.getFinishTime()));
            result.put("format", this.getFormat().name());
            result.put("progress", this.getProgress());
            if (this.getError() != null) {
                result.put("error", this.getError().getMessage());
            }
            return result;
        }
    }

    public static class SearchResult {
        public String query;
        public long total = 0;
        public List<Integer> ids = new ArrayList<>();
        public long ms = 0;
    }

    private static void notifyAll(Map<Integer, WsConnectContext> connections, Map<String, Object> data) throws JsonProcessingException {
        List<Integer> deleteTargets = new ArrayList<>();
        String json = mapper.writeValueAsString(data);
        for (Map.Entry<Integer, WsConnectContext> connection: connections.entrySet()) {
            if (!connection.getValue().session.isOpen()) {
                deleteTargets.add(connection.getValue().session.hashCode());
            } else {
                connection.getValue().send(json);
            }
        }
        for (Integer target: deleteTargets) {
            connections.remove(target);
        }
    }

    private static void notifyAll(Map<Integer, WsConnectContext> connections, Function<WsConnectContext, Map<String, String>> generator) throws JsonProcessingException {
        List<Integer> deleteTargets = new ArrayList<>();
        for (Map.Entry<Integer, WsConnectContext> connection: connections.entrySet()) {
            if (!connection.getValue().session.isOpen()) {
                deleteTargets.add(connection.getValue().session.hashCode());
            } else {
                Map<String, String> data = generator.apply(connection.getValue());
                String json = mapper.writeValueAsString(data);
                connection.getValue().send(json);
            }
        }
        for (Integer target: deleteTargets) {
            connections.remove(target);
        }
    }

    static {
        try {
            lucene = new LuceneManager(
                Settings.getLuceneIndex(),
                Arrays.asList(LuceneFieldKeys.values()).stream().filter(field -> {
                    return Arrays.asList(
                        TextField.class,
                        KeywordField.class
                    ).contains(
                        field.getFieldClass()
                    );
                }).map(
                    field -> field.name()
                ).toList()
            );
            watcher = new SyslogReceiver(Settings.getSyslogPort(), lucene);
            watcher.addEventListener(doc -> {
                try {
                    notifyAll(realtimeConnections, ctx -> 
                        SyslogReceiver.toMap(
                            doc,
                            getZoneOffset(ctx.cookieMap())
                        )
                    );
                } catch (Exception e) {
                    logger.atError().log("ws send error.", e);
                }
            });
            watcher.addEventListener(doc -> {
                if (script != null && !script.trim().isEmpty()) {
                    try (InputStream input = new FileInputStream(script);) {
                        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                        String source = reader.lines().collect(Collectors.joining("\n"));
                        engine.put("doc", doc);
                        engine.eval(source);
                    } catch (Exception e) {
                        logger.atError().log("script eval error.", e);
                    }
                }
            });
            worker = new Thread(watcher);
            worker.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                watcher.stop();
                try {
                    worker.join();
                } catch (InterruptedException e) {
                    logger.atError().log("SyslogReceiver stop failed.", e);
                }
            }));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ZoneOffset getZoneOffset(Map<String, String> cookies) {
        int offset = Integer.valueOf(cookies.get("X-Tz-Offset")) * -1;
        return ZoneOffset.ofHoursMinutes(offset / 60, offset % 60);
    }

    public static void main(String[] args) throws IOException, ParseException, QueryNodeException {
        Settings.print();
        // TODO: 認証(https://javalin.io/tutorials/auth-example)
        Javalin server = Javalin.create(config -> {
            config.staticFiles.enableWebjars();
            config.staticFiles.add(staticFiles -> {
                staticFiles.hostedPath = "/";
                staticFiles.directory = System.getProperty("web.directory", "/public");
                staticFiles.location = Location.valueOf(System.getProperty("web.location", "CLASSPATH"));
            });
        }).get(
            "/api/search", ctx -> ctx.json(search(ctx.queryParam("query"), getZoneOffset(ctx.cookieMap())))
        ).get(
            "/api/config", Main::config
        ).get(
            "/api/documents", Main::documents
        ).get(
            "/api/group/count", Main::groupCount
        ).get(
            "/api/group/count/timeline", Main::timelineCount
        ).post(
            "/api/export/sqlite", Main::exportSqlite  // TODO: キャンセル
        ).post(
            "/api/export/tsv", Main::exportTsv  // TODO: キャンセル
        ).get(
            "/api/download", Main::download
        ).post(
            "/api/import/tsv", Main::importTsv  // TODO: キャンセル
        ).get(
            "/api/job", Main::getJobs
        ).delete(
            "/api/job", Main::removeJob
        ).before(
            ctx -> ctx.attribute("start", new Date().getTime())
        ).after(
            ctx -> logger.atInfo().log("{} {}:{} {}",
                new Date().getTime() - ((long)ctx.attribute("start")),
                ctx.req().getRemoteAddr(),
                ctx.req().getRemotePort(),
                ctx.fullUrl()
            )
        ).exception(Exception.class, (e, ctx) -> {
            logger.error(ctx.fullUrl(), e);
            ctx.status(HttpStatus.INTERNAL_SERVER_ERROR).json(e);
        }).ws("/ws/realtime", ws -> {
            ws.onConnect(ctx -> {
                logger.atInfo().addKeyValue("addr", ctx.host()).log("realtime ws connected.");
                ctx.enableAutomaticPings();
                realtimeConnections.put(ctx.session.hashCode(), ctx);
            });
            ws.onClose(ctx -> {
                logger.atInfo().addKeyValue("addr", ctx.host()).log("realtime ws closed.");
                realtimeConnections.remove(ctx.session.hashCode());
            });
            ws.onError(ctx -> {
                logger.atError().addKeyValue("addr", ctx.host()).log("realtime ws error.");
            });
        }).ws("/ws/job", ws -> {
            ws.onConnect(ctx -> {
                logger.atInfo().addKeyValue("addr", ctx.host()).log("job ws connected.");
                ctx.enableAutomaticPings();
                jobConnections.put(ctx.session.hashCode(), ctx);
            });
            ws.onClose(ctx -> {
                logger.atInfo().addKeyValue("addr", ctx.host()).log("job ws closed.");
                jobConnections.remove(ctx.session.hashCode());
            });
            ws.onError(ctx -> {
                logger.atError().addKeyValue("addr", ctx.host()).log("job ws error.");
            });
        });
        server.start(Settings.getWebPort());
    }

    private static SearchResult search(String query, ZoneOffset offset) throws ParseException, IOException, QueryNodeException {
        long start = new Date().getTime();
        SearchResult result = new SearchResult();
        result.query = query;
        try (LuceneReader reader = lucene.getReader();) {
            TopDocs hits = reader.search(
                LuceneFieldKeys.message.name(),
                result.query,
                new Sort(new SortedNumericSortField(
                    LuceneFieldKeys.sort.name(),
                    SortField.Type.LONG,
                    true
                )),
                LuceneFieldKeys.getPointsConfig(offset)
            );
            result.total = hits.totalHits.value();
            result.ids = Arrays.asList(
                hits.scoreDocs
            ).stream().map(
                hit -> hit.doc
            ).collect(
                Collectors.toList()
            );
            long end = new Date().getTime();
            result.ms = end - start;
            return result;
        } catch (IndexNotFoundException e) {
            logger.atWarn().log("index not found.");
            return new SearchResult();
        }
    }

    private static void config(Context ctx) throws ParseException, IOException, QueryNodeException {
        Map<String, Object> result = new HashMap<>() {{
            this.put("settings", Settings.get());
            this.put("facility", Arrays.asList(Facility.values()).stream().map(item -> item.name()).toList());
            this.put("severity", Arrays.asList(Severity.values()).stream().map(item -> item.name()).toList());
            // TODO: IndexNotFoundException
            // TODO: org.apache.lucene.index.DocValues
            this.put("host", new ArrayList<>() {{
                try (LuceneReader reader = lucene.getReader();) {
                    Map<BytesRef, Long> count = reader.groupCount(
                        LuceneFieldKeys.message.name(),
                        "*:*",
                        LuceneFieldKeys.getPointsConfig(getZoneOffset(ctx.cookieMap())),
                        LuceneFieldKeys.host.name()
                    );
                    this.addAll(count.keySet().stream().map(key -> key.utf8ToString()).sorted().toList());
                }
            }});
            this.put("day", new HashMap<>() {{
                try (LuceneReader reader = lucene.getReader();) {
                    TopDocs hits = reader.search(
                        LuceneFieldKeys.message.name(),
                        "*:*",
                        new Sort(new SortedNumericSortField(
                            LuceneFieldKeys.sort.name(),
                            SortField.Type.LONG,
                            false
                        )),
                        LuceneFieldKeys.getPointsConfig(getZoneOffset(ctx.cookieMap()))
                    );
                    long now = new Date().getTime();
                    if (hits.scoreDocs.length > 0) {
                        this.put("min", Long.valueOf(reader.get(hits.scoreDocs[0].doc).get(LuceneFieldKeys.timestamp.name())));
                        this.put("max", Long.valueOf(reader.get(hits.scoreDocs[hits.scoreDocs.length - 1].doc).get(LuceneFieldKeys.timestamp.name())));
                    } else {
                        this.put("min", now);
                        this.put("max", now);
                    }
                }
            }});
            Locale locale = Locale.of(ctx.header("accept-language").split(",")[0]);
            this.put("monthNames", Arrays.asList(
                Month.values()
            ).stream().map(
                x -> x.getDisplayName(TextStyle.FULL, locale)
            ).toList());
            this.put("monthNamesShort", Arrays.asList(
                Month.values()
            ).stream().map(
                x -> x.getDisplayName(TextStyle.SHORT, locale)
            ).toList());
            List<DayOfWeek> dayOfWeeks = Arrays.asList(
                DayOfWeek.SUNDAY,
                DayOfWeek.MONDAY,
                DayOfWeek.TUESDAY,
                DayOfWeek.WEDNESDAY,
                DayOfWeek.THURSDAY,
                DayOfWeek.FRIDAY,
                DayOfWeek.SATURDAY
            );
            this.put("dayNames", dayOfWeeks.stream().map(
                x -> x.getDisplayName(TextStyle.FULL, locale)
            ).toList());
            this.put("dayNamesMin", dayOfWeeks.stream().map(
                x -> x.getDisplayName(TextStyle.NARROW, locale)
            ).toList());
            this.put("dayNamesShort", dayOfWeeks.stream().map(
                x -> x.getDisplayName(TextStyle.SHORT, locale)
            ).toList());
        }};
        ctx.json(result);
    }

    private static void documents(Context ctx) throws ParseException, IOException, QueryNodeException {
        try {
            SearchResult hits = search(ctx.queryParam("query"), getZoneOffset(ctx.cookieMap()));
            Integer first = ctx.queryParam("first") != null ? Integer.valueOf(ctx.queryParam("first")) : 0;
            Integer last = ctx.queryParam("last") != null ? Integer.valueOf(ctx.queryParam("last")) : hits.ids.size();
            List<Integer> ids = hits.ids.subList(
                first,
                last > hits.ids.size()
                    ? hits.ids.size()
                    : last
            );
            PipedInputStream pin = new PipedInputStream();
            GZIPOutputStream pout = new GZIPOutputStream(new PipedOutputStream(pin));
            new Thread(() -> {
                try (JsonGenerator json = new JsonFactory().createGenerator(pout);) {
                    json.writeStartObject();
                    json.writeNumberField("total", hits.total);
                    json.writeNumberField("ms", hits.ms);
                    json.writeFieldName("docs");
                    json.writeStartArray();
                    try (LuceneReader reader = lucene.getReader();) {
                        for (int id: ids) {
                            Map<String, String> doc = SyslogReceiver.toMap(reader.get(id), getZoneOffset(ctx.cookieMap()));
                            json.writeStartObject();
                            json.writeNumberField("id", id);
                            for (Entry<String, String> entry: doc.entrySet()) {
                                json.writeStringField(entry.getKey(), entry.getValue());
                            }
                            json.writeEndObject();
                            json.flush();
                        }
                    }
                    json.writeEndArray();
                    json.writeEndObject();
                    json.flush();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).start();
            ctx.contentType(
                "application/json"
            ).header(
                "Content-Encoding", "gzip"
            ).result(
                pin
            );
        } catch (IndexNotFoundException e) {
            logger.atWarn().log("index not found.");
        }
    }

    private static void groupCount(Context ctx) throws ParseException, IOException, QueryNodeException {
        try (LuceneReader reader = lucene.getReader();) {
            String field = ctx.queryParam("field");
            String query = ctx.queryParam("query");
            Map<BytesRef, Long> result = reader.groupCount(
                LuceneFieldKeys.message.name(),
                query,
                LuceneFieldKeys.getPointsConfig(getZoneOffset(ctx.cookieMap())),
                field
            );
            Map<String, Long> count = new HashMap<>() {{
                for (Entry<BytesRef, Long> entry: result.entrySet()) {
                    this.put(entry.getKey().utf8ToString(), entry.getValue());
                }
            }};
            ctx.json(count);
        } catch (IndexNotFoundException e) {
            logger.atWarn().log("index not found.");
        }
    }

    private static void timelineCount(Context ctx) throws ParseException, IOException, QueryNodeException {
        try (LuceneReader reader = lucene.getReader();) {
            String query = ctx.queryParam("query");
            Long span = Long.valueOf(ctx.queryParam("span"));
            ZoneOffset offset = getZoneOffset(ctx.cookieMap());
            ZoneId zone = offset.normalized();

            SearchResult hits = search(query, offset);

            if (hits.ids.size() > 0) {
                long first = LuceneFieldKeys.timestamp.get(reader.get(hits.ids.get(hits.ids.size() - 1)), Long.class);
                long last = LuceneFieldKeys.timestamp.get(reader.get(hits.ids.get(0)), Long.class);

                long min = OffsetDateTime
                    .ofInstant(new Date(first).toInstant(), zone)
                    .withHour(0)
                    .withMinute(0)
                    .withSecond(0)
                    .withNano(0)
                    .toInstant()
                    .toEpochMilli();

                long max = OffsetDateTime
                    .ofInstant(new Date(last).toInstant(), zone)
                    .plusDays(1)
                    .withHour(0)
                    .withMinute(0)
                    .withSecond(0)
                    .withNano(0)
                    .toInstant()
                    .toEpochMilli();

                long width = span * 60 * 1000;

                LongRangeGroupSelector selector = new LongRangeGroupSelector(
                    LongValuesSource.fromLongField(LuceneFieldKeys.timestamp.name()),
                    new LongRangeFactory(min, width, max)
                );

                // TODO: 2軸でgroupingする方法
                Map<LongRange, Long> result = reader.groupCount(
                    LuceneFieldKeys.message.name(),
                    query,
                    LuceneFieldKeys.getPointsConfig(offset),
                    selector
                );

                DateTimeFormatter format = DateTimeFormatter.ofPattern(
                    span >= 60 * 24
                    ? "yyyy-MM-dd"
                    : "yyyy-MM-dd HH:mm"
                );

                Function<Long, String> formatter = timestamp -> OffsetDateTime
                    .ofInstant(new Date(timestamp).toInstant(), zone)
                    .format(format);

                Map<String, Long> count = new HashMap<>() {{
                    long current = min;
                    do {
                        this.put(formatter.apply(current), 0l);
                        current += width;
                    } while (current < max);
                }};

                for (Entry<LongRange, Long> entry: result.entrySet()) {
                    String hour = formatter.apply(entry.getKey().min);
                    count.put(hour, entry.getValue());
                }

                ctx.json(count);
            } else {
                ctx.json(new HashMap<>());
            }
        } catch (IndexNotFoundException e) {
            logger.atWarn().log("index not found.");
        }
    }

    private static void exportTsv(Context ctx) throws IOException, ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ParseException, QueryNodeException {
        TempFile temp = new TempFile("logucene_", FileFormat.TSV.getExt());
        ZoneOffset offset = getZoneOffset(ctx.cookieMap());
        SearchResult hits = search(ctx.queryParam("query"), getZoneOffset(ctx.cookieMap()));

        ImportExportJob job = new ImportExportJob(temp, (file, progress) -> {
            try (
                PrintWriter writer = new PrintWriter(new FileOutputStream(temp));
                LuceneReader reader = lucene.getReader();
            ) {
                Function<List<String>, String> format = row -> String.join("\t", row);
                List<String> header = new ArrayList<>();
                for (int count = 0; count < hits.ids.size(); count++) {
                    progress.accept(new Progress(hits.total, count + 1));
                    int id = hits.ids.get(count);
                    Map<String, String> doc = SyslogReceiver.toMap(reader.get(id), offset);
                    List<String> line = new ArrayList<>();
                    if (header.size() == 0) {
                        for (String key: doc.keySet()) {
                            header.add(key);
                        }
                        writer.println(format.apply(header));
                    }
                    for (String key: header) {
                        line.add(doc.get(key));
                    }
                    writer.println(format.apply(line));
                }
            }
        });

        job.setType(JobType.Export);
        job.setFormat(FileFormat.TSV);

        job.onUpdate((progress) -> {
            try {
                notifyAll(jobConnections, job.payload(offset));
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });

        jobs.put(job.hashCode(), job);
        job.start();
    }

    private static void importTsv(Context ctx) throws IOException, NumberFormatException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        // TODO: upsert
        ZoneOffset offset = getZoneOffset(ctx.cookieMap());
        Map<TempFile, Long> files = new HashMap<>();
        for (UploadedFile file: ctx.uploadedFiles()) {
            TempFile temp = new TempFile("logucene_", file.extension());
            long count = 0;
            try (
                InputStream input = new GZIPInputStream(file.content());
                BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                OutputStream output = new GZIPOutputStream(new FileOutputStream(temp));
                PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(output)));
            ) {
                while (reader.ready()) {
                    writer.println(reader.readLine());
                    count++;
                }
            }
            files.put(temp, count);
        }
        for (Entry<TempFile, Long> entry: files.entrySet()) {
            ImportExportJob job = new ImportExportJob(entry.getKey(), (file, progress) -> {
                Iterable<Document> docs = new Iterable<Document>() {
                    @Override
                    public Iterator<Document> iterator() {
                        try {
                            return new Iterator<>() {
                                long count = 1;
                                // TODO: キレイにcloseする方法
                                @SuppressWarnings("resource")
                                BufferedReader reader = new BufferedReader(
                                    new InputStreamReader(
                                        new GZIPInputStream(
                                            new FileInputStream(file)
                                        )
                                    )
                                );
                                List<LuceneFieldKeys> fields = Arrays.asList(
                                    LuceneFieldKeys.timestamp,
                                    LuceneFieldKeys.addr,
                                    LuceneFieldKeys.port,
                                    LuceneFieldKeys.raw
                                );
                                Map<LuceneFieldKeys, Integer> colum = new HashMap<>() {{
                                    if (hasNext()) {
                                        List<String> line = Arrays.asList(reader.readLine().split("\t"));
                                        for (int i = 0; i < line.size(); i++) {
                                            for (LuceneFieldKeys field: fields) {
                                                if (field.name().equals(line.get(i))) {
                                                    this.put(field, i);
                                                }
                                            }
                                        }
                                    }
                                }};
                                @Override
                                public boolean hasNext() {
                                    try {
                                        boolean ready = reader.ready();
                                        if (!ready) {
                                            close();
                                        }
                                        return ready;
                                    } catch (Exception e) {
                                        close();
                                        throw new RuntimeException(e);
                                    }
                                }
                                @Override
                                public Document next() {
                                    try {
                                        progress.accept(new Progress(entry.getValue(), ++count));
                                        List<String> line = Arrays.asList(reader.readLine().split("\t"));
                                        long timestamp = Long.valueOf(line.get(colum.get(LuceneFieldKeys.timestamp)));
                                        return SyslogReceiver.parse(
                                            timestamp,
                                            line.get(colum.get(LuceneFieldKeys.addr)),
                                            Integer.valueOf(line.get(colum.get(LuceneFieldKeys.port))),
                                            line.get(colum.get(LuceneFieldKeys.raw))
                                        );
                                    } catch (Exception e) {
                                        close();
                                        throw new RuntimeException(e);
                                    }
                                }
                                private void close() {
                                    try {
                                        reader.close();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            };
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
                // TODO: この方法だと、途中でエラーが発生した場合、中途半端にデータが登録されるため、
                //       一時ディレクトリでindexを作成し、IndexWriter.addIndexesでマージする
                lucene.add(docs);
            });

            job.setType(JobType.Import);
            job.setFormat(FileFormat.TSV);

            job.onUpdate((progress) -> {
                try {
                    notifyAll(jobConnections, job.payload(offset));
                } catch (JsonProcessingException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });

            jobs.put(job.hashCode(), job);
            job.start();
        }
    }

    private static void exportSqlite(Context ctx) throws IOException, ParseException, QueryNodeException {
        TempFile temp = new TempFile("logucene_", FileFormat.SQLite.getExt());
        ZoneOffset offset = getZoneOffset(ctx.cookieMap());
        SearchResult hits = search(ctx.queryParam("query"), getZoneOffset(ctx.cookieMap()));

        ImportExportJob job = new ImportExportJob(temp, (file, progress) -> {

            String clazz = Settings.getSqliteAnalyzer();
            Analyzer analyzer = (Analyzer) Class.forName(clazz).getDeclaredConstructor().newInstance();

            List<String> fields = Arrays.asList(
                LuceneFieldKeys.values()
            ).stream().map(
                field -> field.name()
            ).toList();

            String ddl = "create virtual table syslog using fts5("
                        + String.join(", ", fields)
                        + ", tokens);";

            String dml = "insert into syslog (rowid, "
                        + String.join(", ", fields)
                        + ", tokens) values (?, "
                        + fields.stream().map(field -> "?").collect(Collectors.joining(", "))
                        + ", ?);";

            Class.forName("org.sqlite.JDBC");

            try (
                Connection connection = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());
                Statement statement = connection.createStatement();
            ) {
                connection.setAutoCommit(false);
                statement.execute(ddl);
                try (
                    LuceneReader reader = lucene.getReader();
                    PreparedStatement insert = connection.prepareStatement(dml);
                ) {
                    for (int count = 0; count < hits.ids.size(); count++) {
                        progress.accept(new Progress(hits.total, count + 1));
                        int id = hits.ids.get(count);
                        Map<String, String> doc = SyslogReceiver.toMap(reader.get(id), offset);
                        String message = doc.get(LuceneFieldKeys.message.name());
                        try (
                            TokenStream tokenizer = analyzer.tokenStream(
                                LuceneFieldKeys.message.name(),
                                message
                            );
                        ) {
                            OffsetAttribute position = tokenizer.getAttribute(OffsetAttribute.class);
                            tokenizer.reset();
                            String tokens = "";
                            while (tokenizer.incrementToken()) {
                                String token = message.substring(position.startOffset(), position.endOffset());
                                if (!"".equals(token)) {
                                    tokens = ("".equals(tokens) ? "" : tokens + " ") + token;
                                }
                            }
                            int i = 1;
                            int j = 0;
                            insert.setInt(i++, id);
                            while (j < fields.size()) {
                                insert.setString(i++, doc.get(fields.get(j++)));
                            }
                            insert.setString(i++, tokens);
                            insert.addBatch();
                            // TODO: マジックナンバー
                            if (count % 1000 == 0) {
                                insert.executeBatch();
                                insert.clearBatch();
                            }
                        }
                    }
                    insert.executeBatch();
                    insert.clearBatch();
                }
                connection.commit();
            }
        });

        job.setType(JobType.Export);
        job.setFormat(FileFormat.SQLite);

        job.onUpdate((progress) -> {
            try {
                notifyAll(jobConnections, job.payload(offset));
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });

        jobs.put(job.hashCode(), job);
        job.start();
    }

    private static void download(Context ctx) throws IOException, ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ParseException, QueryNodeException {
        int id = Integer.valueOf(ctx.queryParam("id"));
        ImportExportJob job = jobs.get(id);
        PipedInputStream pin = new PipedInputStream();
        new Thread(() -> {
            try (
                GZIPOutputStream pout = new GZIPOutputStream(new PipedOutputStream(pin));
                InputStream input = new FileInputStream(job.getData());
            ) {
                while (input.available() > 0) {
                    byte[] buf = input.readNBytes(1024 * 1024);
                    pout.write(buf);
                    pout.flush();
                }
                pout.finish();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } 
        }).start();
        ctx.contentType(
            "application/octet-stream"
        ).header(
            "Content-Disposition", "attachment; filename=\"logucene." + job.getFormat().getExt() + ".gz\""
        ).result(
            pin
        );
    }

    private static void getJobs(Context ctx) {
        ZoneOffset offset = getZoneOffset(ctx.cookieMap());
        ctx.json(new HashMap<>() {{
            for (ImportExportJob job: jobs.values()) {
                this.put(job.hashCode(), job.payload(offset));
            }
        }});
    }

    private static void removeJob(Context ctx) throws JsonProcessingException {
        int id = Integer.valueOf(ctx.queryParam("id"));
        ImportExportJob job = jobs.get(id);
        job.getData().delete();
        jobs.remove(id);
        notifyAll(jobConnections, new HashMap<>() {{
            this.put("type", job.getType().name().toLowerCase());
            this.put("event", Job.Event.remove.name());
            this.put("id", job.hashCode());
        }});
    }
}
