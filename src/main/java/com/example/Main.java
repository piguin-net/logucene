package com.example;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

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

import com.example.LuceneManager.LuceneFieldKeys;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import io.javalin.http.staticfiles.Location;

/**
 * Hello world!
 */
public class Main 
{
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    private static LuceneManager lucene;
    private static SyslogReceiver watcher;
    private static Thread worker;
    private static Map<UUID, List<Integer>> cache = new HashMap<>();  // TODO: メモリリーク

    public static class SearchResult {
        public UUID uuid = UUID.randomUUID();
        public long total = 0;
        public List<Integer> ids = new ArrayList<>();
    }

    static {
        try {
            lucene = new LuceneManager(System.getProperty("lucene.index", "index"));
            watcher = new SyslogReceiver(Integer.getInteger("syslog.port", 1514), lucene);
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
        );
        server.start(Integer.getInteger("web.port", 8080));
    }

    private static void search(Context ctx) {
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
                ))
            );
            result.total = hits.totalHits.value();
            result.ids = Arrays.asList(
                hits.scoreDocs
            ).stream().map(
                hit -> hit.doc
            ).collect(
                Collectors.toList()
            );
            cache.put(result.uuid, result.ids);
            ctx.json(result);
        } catch (IndexNotFoundException e) {
            logger.atWarn().log("index not found.");
            cache.put(result.uuid, new ArrayList<>());
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
                Map<String, Object> dict = new HashMap<>() {{
                    this.put("id", doc.getKey());
                    for (IndexableField field: doc.getValue().getFields()) {
                        this.put(field.name(), doc.getValue().get(field.name()));
                    }
                }};
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
