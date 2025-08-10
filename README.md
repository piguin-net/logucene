# 概要
syslogを受け取り、luceneへ保存します。
保存されているログをブラウザから参照します。

# 使い方
```
mvn package
java -jar target/logucene-1.0-SNAPSHOT-jar-with-dependencies.jar
```

# オプション
```
java \
  -Dsyslog.port=1514 \
  -Dweb.port=8080 \
  -Dlucene.index=index \
  -Dlucene.analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer \
  -Duser.timezone=Asia/Tokyo \
  -jar target/logucene-1.0-SNAPSHOT-jar-with-dependencies.jar
```
| Key             | Value                                  |
| --------------- | -------------------------------------- |
| syslog.port     | syslogの受信ポート(UDP)                |
| web.port        | webサーバの待受ポート                  |
| lucene.index    | luceneの保存先ディレクトリ             |
| lucene.analyzer | 全文検索に使用するアナライザ           |
| user.timezone   | ログ受信日時を保存する際のタイムゾーン |
