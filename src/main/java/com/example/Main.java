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
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexableField;
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
            lucene = new LuceneManager(Settings.getLuceneIndex());
            watcher = new SyslogReceiver(Settings.getSyslogPort(), lucene);
            watcher.addEventListener(doc -> {
                try {
                    List<Integer> deleteTargets = new ArrayList<>();
                    String json = mapper.writeValueAsString(convert(doc));
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
            "/api/search", ctx -> ctx.json(search(ctx.queryParam("query")))
        ).get(
            "/api/config", Main::config
        ).get(
            "/api/documents", Main::documents
        ).get(
            "/api/count", Main::count
        ).get(
            "/api/export/sqlite", Main::sqlite
        ).get(
            "/api/export/tsv", Main::tsv
        ).before(
            ctx -> ctx.attribute("start", ZonedDateTime.now().toInstant().toEpochMilli())
        ).after(
            ctx -> logger.atInfo().log("{} {}:{} {}",
                ZonedDateTime.now().toInstant().toEpochMilli() - ((long)ctx.attribute("start")),
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

    private static Map<String, Object> convert(Document doc) {
        return new HashMap<>() {{
            for (IndexableField field: doc.getFields()) {
                this.put(field.name(), doc.get(field.name()));
            }
        }};
    }

    private static SearchResult search(String query) throws ParseException, IOException, QueryNodeException {
        long start = ZonedDateTime.now().toInstant().toEpochMilli();
        SearchResult result = new SearchResult();
        result.query = query;
        try (LuceneReader reader = lucene.getReader();) {
            TopDocs hits = reader.search(
                LuceneFieldKeys.message.name(),
                result.query,
                new Sort(new SortedNumericSortField(
                    LuceneFieldKeys.timestamp.name(),
                    SortField.Type.LONG,
                    true
                )),
                LuceneFieldKeys.getPointsConfig()
            );
            result.total = hits.totalHits.value();
            result.ids = Arrays.asList(
                hits.scoreDocs
            ).stream().map(
                hit -> hit.doc
            ).collect(
                Collectors.toList()
            );
            long end = ZonedDateTime.now().toInstant().toEpochMilli();
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
                    Map<BytesRef, Long> count = reader.count(
                        LuceneFieldKeys.message.name(),
                        "*:*",
                        LuceneFieldKeys.getPointsConfig(),
                        LuceneFieldKeys.host.name()
                    );
                    this.addAll(count.keySet().stream().map(key -> key.utf8ToString()).toList());
                }
            }});
        }};
        ctx.json(result);
    }

    private static void documents(Context ctx) throws ParseException, IOException, QueryNodeException {
        try {
            SearchResult hits = search(ctx.queryParam("query"));
            Integer first = ctx.queryParam("first") != null ? Integer.valueOf(ctx.queryParam("first")) : 0;
            Integer last = ctx.queryParam("last") != null ? Integer.valueOf(ctx.queryParam("last")) : hits.ids.size();
            List<String> fields = ctx.queryParam("fields") != null
                ? Arrays.asList(ctx.queryParam("fields").split(","))
                : Arrays.asList(LuceneFieldKeys.values()).stream().map(
                    field -> field.name()
                ).toList();
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
                            Document doc = reader.get(id);
                            json.writeStartObject();
                            json.writeNumberField("id", id);
                            for (String field: fields) {
                                json.writeStringField(field, doc.get(field));
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

    private static void count(Context ctx) throws ParseException, IOException, QueryNodeException {
        try (LuceneReader reader = lucene.getReader();) {
            String field = ctx.queryParam("field");
            Map<BytesRef, Long> result = reader.count(
                LuceneFieldKeys.message.name(),
                ctx.queryParam("query"),
                LuceneFieldKeys.getPointsConfig(),
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

    private static void tsv(Context ctx) throws IOException, ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ParseException, QueryNodeException {
        SearchResult hits = search(ctx.queryParam("query"));
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
                for (LuceneFieldKeys field: LuceneFieldKeys.values()) {
                    header.add(field.name());
                }
                writer.println(format.apply(header));
                for (int id: hits.ids) {
                    Document doc = reader.get(id);
                    List<String> line = new ArrayList<>();
                    for (LuceneFieldKeys field: LuceneFieldKeys.values()) {
                        line.add(doc.get(field.name()));
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

    // TODO; 時間が掛かるためジョブまたはCLI
    private static void sqlite(Context ctx) throws IOException, ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ParseException, QueryNodeException {
        SearchResult hits = search(ctx.queryParam("query"));

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
                    Document doc = reader.get(id);
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
