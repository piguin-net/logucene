package com.example;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
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
    private static String script = System.getProperty("syslog.listener", null);
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
            lucene = new LuceneManager(System.getProperty("lucene.index", "index"));
            watcher = new SyslogReceiver(Integer.getInteger("syslog.port", 1514), lucene);
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
                if (script != null) {
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

    public static void main(String[] args) {
        Javalin server = Javalin.create(config -> {
            config.staticFiles.add(staticFiles -> {
                staticFiles.hostedPath = "/";
                staticFiles.directory = "/public";
                staticFiles.location = Location.CLASSPATH;
            });
        }).get(
            "/api/search", ctx -> ctx.json(search(ctx.queryParam("query")))
        ).get(
            "/api/config", Main::config
        ).get(
            "/api/documents", Main::documents
        ).get(
            "/api/download/sqlite", Main::sqlite
        ).get(
            "/api/download/excel", Main::excel
        ).before(
            ctx -> logger.atInfo().log(ctx.fullUrl())
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
        server.start(Integer.getInteger("web.port", 8080));
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
        try {
            TopDocs hits = lucene.search(
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
            SearchResult hits = search("*:*");
            this.put("facility", Arrays.asList(Facility.values()).stream().map(item -> item.name()).toList());
            this.put("severity", Arrays.asList(Severity.values()).stream().map(item -> item.name()).toList());
            this.put("host", new HashSet<>() {{
                try (LuceneReader reader = lucene.getReader();) {
                    for (int id: hits.ids) {
                        Document doc = reader.get(id);
                        this.add(doc.get("host"));
                    }
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
            List<Integer> ids = hits.ids.subList(
                first,
                last > hits.ids.size()
                    ? hits.ids.size()
                    : last
            );
            PipedInputStream pin = new PipedInputStream();
            PipedOutputStream pout = new PipedOutputStream(pin);
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
                            for (LuceneFieldKeys field: LuceneFieldKeys.values()) {
                                json.writeStringField(field.name(), doc.get(field.name()));
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
            ctx.result(pin);
        } catch (IndexNotFoundException e) {
            logger.atWarn().log("index not found.");
        }
    }

    private static void excel(Context ctx) throws IOException, ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ParseException, QueryNodeException {
        SearchResult hits = search(ctx.queryParam("query"));

        try (Workbook book = new XSSFWorkbook();) {
            Sheet sheet = book.createSheet();
            int x = 0;
            int y = 0;
            Row header = sheet.createRow(y++);
            header.createCell(x++).setCellValue("id");
            for (LuceneFieldKeys field: LuceneFieldKeys.values()) {
                header.createCell(x++).setCellValue(field.name());
            }
            try (LuceneReader reader = lucene.getReader();) {
                for (int id: hits.ids) {
                    Document doc = reader.get(id);
                    x = 0;
                    Row row = sheet.createRow(y++);
                    row.createCell(x++).setCellValue(id);
                    for (LuceneFieldKeys field: LuceneFieldKeys.values()) {
                        row.createCell(x++).setCellValue(doc.get(field.name()));
                    }
                }
            }
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            book.write(buf);
            ctx.contentType(
                "application/octet-stream"
            ).header(
                "Content-Disposition", "attachment; filename=\"logucene.xlsx\""
            ).result(
                buf.toByteArray()
            );
        }
    }

    private static void sqlite(Context ctx) throws IOException, ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ParseException, QueryNodeException {
        SearchResult hits = search(ctx.queryParam("query"));

        String clazz = System.getProperty("sqlite.analyzer", "org.apache.lucene.analysis.standard.StandardAnalyzer");
        Analyzer analyzer = (Analyzer) Class.forName(clazz).getDeclaredConstructor().newInstance();

        List<String> fields = Stream.concat(
            Arrays.asList("id").stream(),
            Arrays.asList(LuceneFieldKeys.values()).stream().map(field -> field.name())
        ).toList();

        String table = "create table syslog ("
                     + String.join(", ", fields)
                     + ");";

        String virtual = "create virtual table syslog_fts using fts5(message);";

        String insert = "insert into syslog ("
                      + String.join(", ", fields)
                      + ") values ("
                      + fields.stream().map(field -> "?").collect(Collectors.joining(", "))
                      + ");";

        String fts = "insert into syslog_fts (rowid, message) values (?, ?);";

        List<String> indexes = fields.stream().filter(
            field -> !Arrays.asList(
                LuceneFieldKeys.message.name(),
                LuceneFieldKeys.raw.name()
            ).contains(field)
        ).map(
            field -> "create index idx_syslog_" + field + " on syslog(" + field + ");"
        ).toList();

        Class.forName("org.sqlite.JDBC");

        try (
            TempFile temp = new TempFile("logucene_", ".sqlite3");
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + temp.getAbsolutePath());
            Statement statement = connection.createStatement();
        ) {
            connection.setAutoCommit(false);
            statement.execute(table);
            statement.execute(virtual);
            for (String index: indexes) {
                statement.execute(index);
            }
            try (LuceneReader reader = lucene.getReader();) {
                for (int id: hits.ids) {
                    Document doc = reader.get(id);
                    try (PreparedStatement sql = connection.prepareStatement(insert);) {
                        sql.setInt(1, id);
                        for (int i = 1; i < fields.size(); i++) {
                            sql.setString(i + 1, doc.get(fields.get(i)));
                        }
                        sql.execute();
                    }
                    String message = doc.get(LuceneFieldKeys.message.name());
                    try (
                        PreparedStatement sql = connection.prepareStatement(fts);
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
                        sql.setInt(1, id);
                        sql.setString(2, tokens);
                        sql.execute();
                    }
                }
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
