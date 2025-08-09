package com.example;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

public class LuceneManager implements Closeable {

    public static enum LuceneFieldKeys {
        timestamp,
        day,
        time,
        addr,
        port,
        facility,
        severity,
        message;
    };

    private FSDirectory dir;
    private IndexWriter writer;
    private Analyzer analyzer;

    public LuceneManager(String path) throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
        this.dir = FSDirectory.open(Paths.get(path));

        String clazz = System.getProperty("analyzer", "org.apache.lucene.analysis.standard.StandardAnalyzer");
        this.analyzer = (Analyzer) Class.forName(clazz).getDeclaredConstructor().newInstance();

        IndexWriterConfig iwc = new IndexWriterConfig(this.analyzer);
        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
        this.writer = new IndexWriter(dir, iwc);
    }

    public void add(Document doc) throws IOException {
        this.writer.addDocument(doc);
        this.writer.flush();
        this.writer.commit();
    }

    public TopDocs search(String field, String query, Sort order) throws ParseException, IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir);) {
            IndexSearcher searcher = new IndexSearcher(reader);
            QueryParser parser = new QueryParser(field, this.analyzer);
            return searcher.search(parser.parse(query), Integer.MAX_VALUE, order);
        }
    }

    public List<Map.Entry<Integer,Document>> documents(List<Integer> ids) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir);) {
            IndexSearcher searcher = new IndexSearcher(reader);
            StoredFields storedFields = searcher.storedFields();
            List<Map.Entry<Integer,Document>> docs = new ArrayList<>();  // TODO: 消費メモリ
            for (int id: ids) {
                Document doc = storedFields.document(id);
                docs.add(Map.entry(id, doc));
            }
            return docs;
        }
    }

    public Path getDirectory() {
        return this.dir == null ? null : this.dir.getDirectory();
    }

    @Override
    public void close() throws IOException {
        this.writer.close();
    }
}
