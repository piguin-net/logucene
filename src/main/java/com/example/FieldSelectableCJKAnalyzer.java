package com.example;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.cjk.CJKBigramFilter;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.IOUtils;

public class FieldSelectableCJKAnalyzer extends Analyzer {

  /** An immutable stopword set */
  protected final CharArraySet stopwords;

  private List<String> ignoreTokenizeFields = new ArrayList<>();
  private int maxTokenLength = WhitespaceTokenizer.DEFAULT_MAX_WORD_LEN;

  /**
   * File containing default CJK stopwords.
   *
   * <p>Currently it contains some common English words that are not usually useful for searching
   * and some double-byte interpunctions.
   */
  public static final String DEFAULT_STOPWORD_FILE = "stopwords.txt";

  /**
   * Returns an unmodifiable instance of the default stop-words set.
   *
   * @return an unmodifiable instance of the default stop-words set.
   */
  public static CharArraySet getDefaultStopSet() {
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }

  private static class DefaultSetHolder {
    static final CharArraySet DEFAULT_STOP_SET;

    static {
      try {
        DEFAULT_STOP_SET =
            WordlistLoader.getWordSet(
                IOUtils.requireResourceNonNull(
                    CJKAnalyzer.class.getResourceAsStream(DEFAULT_STOPWORD_FILE),
                    DEFAULT_STOPWORD_FILE),
                "#");
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new UncheckedIOException("Unable to load default stopword set", ex);
      }
    }
  }

  /** Builds an analyzer which removes words in {@link #getDefaultStopSet()}. */
  public FieldSelectableCJKAnalyzer() {
    this(DefaultSetHolder.DEFAULT_STOP_SET);
  }

  /**
   * Builds an analyzer with the given stop words
   *
   * @param stopwords a stopword set
   */
  public FieldSelectableCJKAnalyzer(CharArraySet stopwords) {
    super(new ReuseStrategy() {
      private Map<String, TokenStreamComponents> cache = new HashMap<>();
      @Override
      public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
        return this.cache.containsKey(fieldName) ? this.cache.get(fieldName) : null;
      }
      @Override
      public void setReusableComponents(Analyzer analyzer, String fieldName, TokenStreamComponents components) {
        this.cache.put(fieldName, components);
      }
    });
    this.stopwords =
        stopwords == null
            ? CharArraySet.EMPTY_SET
            : CharArraySet.unmodifiableSet(CharArraySet.copy(stopwords));
  }

  public FieldSelectableCJKAnalyzer(List<String> ignoreTokenizeFields) {
    this(DefaultSetHolder.DEFAULT_STOP_SET);
    this.ignoreTokenizeFields = ignoreTokenizeFields;
  }

  public FieldSelectableCJKAnalyzer(CharArraySet stopwords, List<String> ignoreTokenizeFields) {
    this(stopwords);
    this.ignoreTokenizeFields = ignoreTokenizeFields;
  }

  public List<String> getIgnoreTokenizeFields() {
    return this.ignoreTokenizeFields;
  }

  public void setIgnoreTokenizeFields(List<String> ignoreTokenizeFields) {
    this.ignoreTokenizeFields = ignoreTokenizeFields;
  }

  public int getMaxTokenLength() {
    return maxTokenLength;
  }

  public void setMaxTokenLength(int maxTokenLength) {
    this.maxTokenLength = maxTokenLength;
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    if (this.ignoreTokenizeFields.contains(fieldName)) {
      return new TokenStreamComponents(new WhitespaceTokenizer(this.maxTokenLength));
    } else {
      final Tokenizer source = new StandardTokenizer();
      // run the widthfilter first before bigramming, it sometimes combines characters.
      TokenStream result = new CJKWidthFilter(source);
      result = new LowerCaseFilter(result);
      result = new CJKBigramFilter(result);
      return new TokenStreamComponents(source, new StopFilter(result, stopwords));
    }
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    TokenStream result = new CJKWidthFilter(in);
    result = new LowerCaseFilter(result);
    return result;
  }
}
