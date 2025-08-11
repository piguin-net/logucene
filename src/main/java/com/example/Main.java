package com.example;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexableField;
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

import com.example.SyslogReceiver.LuceneFieldKeys;
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
    private static Map<UUID, List<Integer>> cache = new HashMap<>();  // TODO: メモリリーク
    private static Map<Integer, WsConnectContext> connections = new HashMap<>();
    private static String script = System.getProperty("syslog.listener", null);
    private static ScriptEngineManager manager = new ScriptEngineManager();
    private static ScriptEngine engine = manager.getEngineByName("groovy");

    public static class SearchResult {
        public UUID uuid = UUID.randomUUID();
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
            "/api/search", Main::search
        ).get(
            "/api/documents", Main::documents
        ).get(
            "/api/download", Main::download
        ).before(
            ctx -> logger.atInfo().log(ctx.fullUrl())
        ).ws("/ws/realtime", ws -> {
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

    private static void search(Context ctx) {
        long start = ZonedDateTime.now().toInstant().toEpochMilli();
        SearchResult result = new SearchResult();
        try {
            String query = ctx.queryParam("query");
            TopDocs hits = lucene.search(
                LuceneFieldKeys.message.name(),
                query == null || "".equals(query.trim()) ? "*:*" : query,
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
            cache.put(result.uuid, result.ids);
            ctx.json(result);
        } catch (IndexNotFoundException e) {
            logger.atWarn().log("index not found.");
            cache.put(result.uuid, new ArrayList<>());
            ctx.json(result);
        } catch (Exception e) {
            ctx.status(HttpStatus.INTERNAL_SERVER_ERROR).json(e);
        }
    }

    private static void documents(Context ctx) {
        try {
            UUID uuid = UUID.fromString(ctx.queryParam("uuid"));
            Integer first = Integer.valueOf(ctx.queryParam("first"));
            Integer last = Integer.valueOf(ctx.queryParam("last"));
            List<Integer> ids = cache.get(uuid).subList(
                first,
                last > cache.get(uuid).size()
                    ? cache.get(uuid).size()
                    : last
            );
            List<Map.Entry<Integer,Document>> docs = lucene.documents(ids);
            List<Map<String, Object>> result = docs.stream().map(doc -> {
                Map<String, Object> dict = convert(doc.getValue());
                dict.put("id", doc.getKey());
                return dict;
            }).collect(Collectors.toList());
            ctx.json(result);
        } catch (IndexNotFoundException e) {
            logger.atWarn().log("index not found.");
        } catch (Exception e) {
            ctx.status(HttpStatus.INTERNAL_SERVER_ERROR).json(e);
        }
    }

    private static void download(Context ctx) throws IOException {
        UUID uuid = UUID.fromString(ctx.queryParam("uuid"));
        List<Map.Entry<Integer,Document>> docs = lucene.documents(cache.get(uuid));

        try (Workbook book = new XSSFWorkbook();) {
            Sheet sheet = book.createSheet();
            int x = 0;
            int y = 0;
            Row header = sheet.createRow(y++);
            header.createCell(x++).setCellValue("id");
            for (LuceneFieldKeys field: LuceneFieldKeys.values()) {
                header.createCell(x++).setCellValue(field.name());
            }
            for (Map.Entry<Integer,Document> doc: docs) {
                x = 0;
                Row row = sheet.createRow(y++);
                row.createCell(x++).setCellValue(doc.getKey());
                for (LuceneFieldKeys field: LuceneFieldKeys.values()) {
                    row.createCell(x++).setCellValue(doc.getValue().get(field.name()));
                }
            }
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            book.write(buf);
            ctx.contentType(
                "application/octet-stream"
            ).header(
                "Content-Disposition", "attachment; filename=\"syslog.xlsx\""
            ).result(
                buf.toByteArray()
            );
        }
    }
}
