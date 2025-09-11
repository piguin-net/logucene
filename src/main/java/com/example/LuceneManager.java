package com.example;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Analyzer.ReuseStrategy;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
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
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.GroupingSearch;
import org.apache.lucene.search.grouping.LongRange;
import org.apache.lucene.search.grouping.LongRangeGroupSelector;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.store.FSDirectory;

public class LuceneManager implements Closeable {

    private FSDirectory dir;
    private IndexWriter writer;
    private Analyzer writerAnalyzer;
    private Analyzer readerAnalyzer;

    // TODO: 全てのデータを1つのindexディレクトリに保存するなら古いデータの削除機能が欲しい、月毎などで分けたほうがいい？
    public static class LuceneReader implements Closeable {

        private static Object analyzerLock = new Object();
        private final DirectoryReader reader;
        private final IndexSearcher searcher;
        private final Analyzer analyzer;

        public LuceneReader(String path, Analyzer analyzer) throws IOException {
            FSDirectory dir = FSDirectory.open(Paths.get(path));
            this.reader = DirectoryReader.open(dir);
            this.searcher = new IndexSearcher(this.reader);
            this.analyzer = analyzer;
        }

        public TopDocs search(String field, String query, Sort order, Map<String, PointsConfig> pointsConfig) throws ParseException, IOException, QueryNodeException {
            StandardQueryParser parser = new StandardQueryParser(this.analyzer);
            parser.setPointsConfigMap(pointsConfig);
            synchronized (analyzerLock) {
                return this.searcher.search(parser.parse(query, field), Integer.MAX_VALUE, order);
            }
        }

        public Document get(Integer id) throws IOException {
            StoredFields storedFields = this.searcher.storedFields();
            return storedFields.document(id);
        }

        public <BytesRef> Map<BytesRef, Long> groupCount(String field, String query, Map<String, PointsConfig> pointsConfig, String groupField) throws IOException, QueryNodeException {
            GroupingSearch groupingSearch = new GroupingSearch(groupField);
            StandardQueryParser parser = new StandardQueryParser(this.analyzer);
            parser.setPointsConfigMap(pointsConfig);
            Map<BytesRef, Long> count = new HashMap<>();
            int offset = 0;
            int limit = 1024;
            while (true) {
                synchronized (analyzerLock) {
                    TopGroups<BytesRef> result = groupingSearch.search(
                        this.searcher,
                        parser.parse(query, field),
                        offset,
                        limit
                    );
                    if (result.groups.length == 0) break;
                    offset += limit;
                    for (GroupDocs<BytesRef> group: result.groups) {
                        count.put(group.groupValue(), group.totalHits().value());
                    }
                }
            }
            return count;
        }

        public Map<LongRange, Long> groupCount(String field, String query, Map<String, PointsConfig> pointsConfig, LongRangeGroupSelector selector) throws IOException, QueryNodeException {
            GroupingSearch groupingSearch = new GroupingSearch(selector);
            StandardQueryParser parser = new StandardQueryParser(this.analyzer);
            parser.setPointsConfigMap(pointsConfig);
            Map<LongRange, Long> count = new HashMap<>();
            int offset = 0;
            int limit = 1024;
            while (true) {
                synchronized (analyzerLock) {
                    TopGroups<LongRange> result = groupingSearch.search(
                        this.searcher,
                        parser.parse(query, field),
                        offset,
                        limit
                    );
                    if (result.groups.length == 0) break;
                    offset += limit;
                    for (GroupDocs<LongRange> group: result.groups) {
                        count.put(group.groupValue(), group.totalHits().value());
                    }
                }
            }
            return count;
        }

        @Override
        public void close() throws IOException {
            this.reader.close();
        }
    }

    public LuceneManager(String path) throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
        this(path, new ArrayList<>());
    }

    public LuceneManager(String path, List<String> tokenizeFields) throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
        this.dir = FSDirectory.open(Paths.get(path));

        String clazz = Settings.getLuceneAnalyzer();
        this.writerAnalyzer = (Analyzer) Class.forName(clazz).getDeclaredConstructor().newInstance();
        this.readerAnalyzer = new AnalyzerWrapper(new ReuseStrategy() {
            private Map<String, TokenStreamComponents> cache = new HashMap<>();
            @Override
            public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
                return this.cache.containsKey(fieldName) ? this.cache.get(fieldName) : null;
            }
            @Override
            public void setReusableComponents(Analyzer analyzer, String fieldName, TokenStreamComponents components) {
                this.cache.put(fieldName, components);
            }
        }) {
            @Override
            protected Analyzer getWrappedAnalyzer(String fieldName) {
                if (tokenizeFields.contains(fieldName)) {
                    return writerAnalyzer;
                } else {
                    // TODO: 設定で指定可能にする
                    return new WhitespaceAnalyzer();
                }
            }
        };

        IndexWriterConfig iwc = new IndexWriterConfig(this.writerAnalyzer);
        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
        this.writer = new IndexWriter(dir, iwc);
    }

    public void add(Document doc) throws IOException {
        this.writer.addDocument(doc);
        this.writer.flush();
        this.writer.commit();
    }

    public void add(Iterable<Document> docs) throws IOException {
        this.writer.addDocuments(docs);
        this.writer.flush();
        this.writer.commit();
    }

    public LuceneReader getReader() throws IOException {
        return new LuceneReader(dir.getDirectory().toAbsolutePath().toString(), this.readerAnalyzer);
    }

    public Path getDirectory() {
        return this.dir == null ? null : this.dir.getDirectory();
    }

    @Override
    public void close() throws IOException {
        this.writer.close();
    }

    // TODO: 頻出単語を取得
}
