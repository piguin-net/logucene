package com.example;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

public class LuceneManager implements Closeable {

    private FSDirectory dir;
    private IndexWriter writer;
    private Analyzer analyzer;

    public static class LuceneReader implements Closeable {

        private final DirectoryReader reader;
        private final IndexSearcher searcher;

        public LuceneReader(FSDirectory dir) throws IOException {
            this.reader = DirectoryReader.open(dir);
            this.searcher = new IndexSearcher(reader);
        }

        public Document get(Integer id) throws IOException {
            StoredFields storedFields = searcher.storedFields();
            return storedFields.document(id);
        } 

        @Override
        public void close() throws IOException {
            this.reader.close();
        }
        
    }

    public LuceneManager(String path) throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
        this.dir = FSDirectory.open(Paths.get(path));

        String clazz = System.getProperty("lucene.analyzer", "org.apache.lucene.analysis.standard.StandardAnalyzer");
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

    public TopDocs search(String field, String query, Sort order, Map<String, PointsConfig> pointsConfig) throws ParseException, IOException, QueryNodeException {
        try (DirectoryReader reader = DirectoryReader.open(dir);) {
            IndexSearcher searcher = new IndexSearcher(reader);
            StandardQueryParser parser = new StandardQueryParser(this.analyzer);
            parser.setPointsConfigMap(pointsConfig);
            return searcher.search(parser.parse(query, field), Integer.MAX_VALUE, order);
        }
    }

    public LuceneReader getReader() throws IOException {
        return new LuceneReader(dir);
    }

    public Path getDirectory() {
        return this.dir == null ? null : this.dir.getDirectory();
    }

    @Override
    public void close() throws IOException {
        this.writer.close();
    }
}
