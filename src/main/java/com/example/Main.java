package com.example;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.LuceneManager.LuceneReader;
import com.example.SyslogParser.Facility;
import com.example.SyslogParser.Severity;
import com.example.SyslogReceiver.LuceneFieldKeys;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
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
    private static Map<Integer, WsConnectContext> connections = new HashMap<>();
    private static String script = Settings.getSyslogListener();
    private static ScriptEngineManager manager = new ScriptEngineManager();
    private static ScriptEngine engine = manager.getEngineByName("groovy");

    public static class SearchResult {
        public String query;
        public long total = 0;
        public List<Integer> ids = new ArrayList<>();
        public long ms = 0;
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
                    List<Integer> deleteTargets = new ArrayList<>();
                    for (Map.Entry<Integer, WsConnectContext> connection: connections.entrySet()) {
                        String json = mapper.writeValueAsString(SyslogReceiver.toMap(doc, getZoneOffset(connection.getValue().cookieMap())));
                        if (!connection.getValue().session.isOpen()) {
                            deleteTargets.add(connection.getValue().session.hashCode());
                        } else {
                            connection.getValue().send(json);
                        }
                    }
                    for (Integer target: deleteTargets) {
                        connections.remove(target);
                    }
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
            "/api/export/sqlite", Main::sqlite
        ).get(
            "/api/export/tsv", Main::exportTsv
        ).post(
            "/api/import/tsv", Main::importTsv
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
                logger.atInfo().addKeyValue("addr", ctx.host()).log("ws connected.");
                ctx.enableAutomaticPings();
                connections.put(ctx.session.hashCode(), ctx);
            });
            ws.onClose(ctx -> {
                logger.atInfo().addKeyValue("addr", ctx.host()).log("ws closed.");
                connections.remove(ctx.session.hashCode());
            });
            ws.onError(ctx -> {
                logger.atError().addKeyValue("addr", ctx.host()).log("ws error.");
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
                    LuceneFieldKeys.order.name(),
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
            this.put("host", new ArrayList<>() {{
                try (LuceneReader reader = lucene.getReader();) {
                    Map<BytesRef, Long> count = reader.groupCount(
                        LuceneFieldKeys.message.name(),
                        "*:*",
                        LuceneFieldKeys.getPointsConfig(getZoneOffset(ctx.cookieMap())),
                        LuceneFieldKeys.host.name()
                    );
                    this.addAll(count.keySet().stream().map(key -> key.utf8ToString()).toList());
                }
            }});
            this.put("day", new HashMap<>() {{
                try (LuceneReader reader = lucene.getReader();) {
                    TopDocs hits = reader.search(
                        LuceneFieldKeys.message.name(),
                        "*:*",
                        new Sort(new SortedNumericSortField(
                            LuceneFieldKeys.order.name(),
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
            Map<BytesRef, Long> result = reader.groupCount(
                LuceneFieldKeys.message.name(),
                ctx.queryParam("query"),
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

    private static void exportTsv(Context ctx) throws IOException, ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ParseException, QueryNodeException {
        SearchResult hits = search(ctx.queryParam("query"), getZoneOffset(ctx.cookieMap()));
        PipedInputStream pin = new PipedInputStream();
        new Thread(() -> {
            try (
                PrintWriter writer = new PrintWriter(new GZIPOutputStream(new PipedOutputStream(pin)));
                LuceneReader reader = lucene.getReader();
            ) {
                Function<List<String>, String> format = row -> row.stream().map(
                    cell -> cell.replace("\r", "\\r")
                ).map(
                    cell -> cell.replace("\n", "\\n")
                ).map(
                    cell -> cell.replace("\t", "\\t")
                ).collect(
                    Collectors.joining("\t")
                );
                List<String> header = new ArrayList<>();
                for (int id: hits.ids) {
                    List<String> line = new ArrayList<>();
                    Map<String, String> doc = SyslogReceiver.toMap(reader.get(id), getZoneOffset(ctx.cookieMap()));
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
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();
        ctx.contentType(
            "application/octet-stream"
        ).header(
            "Content-Disposition", "attachment; filename=\"logucene.tsv.gz\""
        ).result(
            pin
        );
    }

    private static void importTsv(Context ctx) throws IOException, NumberFormatException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        int chunk = Integer.getInteger("lucene.migration.chunk", 1000);
        for (UploadedFile file: ctx.uploadedFiles()) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(file.content())));) {
                Map<LuceneFieldKeys, Integer> colum = null;
                List<LuceneFieldKeys> target = Arrays.asList(
                    LuceneFieldKeys.timestamp,
                    LuceneFieldKeys.addr,
                    LuceneFieldKeys.port,
                    LuceneFieldKeys.raw
                );
                List<Document> docs = new ArrayList<>();
                while (reader.ready()) {
                    List<String> line = Arrays.asList(reader.readLine().split("\t")).stream().map(
                        cell -> cell.replace("\\r", "\r")
                    ).map(
                        cell -> cell.replace("\\n", "\n")
                    ).map(
                        cell -> cell.replace("\\t", "\t")
                    ).toList();
                    if (colum == null) {
                        colum = new HashMap<>();
                        for (int i = 0; i < line.size(); i++) {
                            LuceneFieldKeys field = LuceneFieldKeys.valueOf(line.get(i));
                            if (target.contains(field)) {
                                colum.put(field, i);
                            }
                        }
                    } else {
                        long timestamp = Long.valueOf(line.get(colum.get(LuceneFieldKeys.timestamp)));
                        Document doc = SyslogReceiver.parse(
                            timestamp,
                            line.get(colum.get(LuceneFieldKeys.addr)),
                            Integer.valueOf(line.get(colum.get(LuceneFieldKeys.port))),
                            line.get(colum.get(LuceneFieldKeys.raw))
                        );
                        docs.add(doc);
                        if (docs.size() == chunk) {
                            lucene.add(docs);
                            docs.clear();
                        }
                    }
                }
                if (docs.size() > 0) {
                    lucene.add(docs);
                }
            }
        }
    }

    // TODO; 時間が掛かるためジョブまたはCLI
    private static void sqlite(Context ctx) throws IOException, ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ParseException, QueryNodeException {
        SearchResult hits = search(ctx.queryParam("query"), getZoneOffset(ctx.cookieMap()));

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
            TempFile temp = new TempFile("logucene_", ".sqlite3");
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + temp.getAbsolutePath());
            Statement statement = connection.createStatement();
        ) {
            connection.setAutoCommit(false);
            statement.execute(ddl);
            try (
                LuceneReader reader = lucene.getReader();
                PreparedStatement insert = connection.prepareStatement(dml);
            ) {
                for (int count = 0; count < hits.ids.size(); count++) {
                    int id = hits.ids.get(count);
                    Map<String, String> doc = SyslogReceiver.toMap(reader.get(id), getZoneOffset(ctx.cookieMap()));
                    String message = doc.get(LuceneFieldKeys.message.name());
                    try (
                        TokenStream tokenizer = analyzer.tokenStream(
                            LuceneFieldKeys.message.name(),
                            message
                        );
                    ) {
                        OffsetAttribute offset = tokenizer.getAttribute(OffsetAttribute.class);
                        tokenizer.reset();
                        String tokens = "";
                        while (tokenizer.incrementToken()) {
                            String token = message.substring(offset.startOffset(), offset.endOffset());
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
            ctx.contentType(
                "application/octet-stream"
            ).header(
                "Content-Disposition", "attachment; filename=\"logucene.sqlite3.gz\""
            ).result(
                new GzipCompressInputStream(new FileInputStream(temp))
            );
        }
    }
}
