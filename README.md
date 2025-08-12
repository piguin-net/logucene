# 概要
syslogをUDPで受け取り、luceneへ保存します。  
ログをブラウザから検索、websocketでリアルタイム表示します。  
RaspberryPiZeroでも動かせそうな、低消費メモリなsyslog管理ツールを探しており、  
メモリの消費を抑えられそうな、Python等のインタプリタ言語＋SQLiteFTSを検討していたのですが、  
luceneを使ってみたかったため、転職活動の一環として、こちらを作成しました。

# 使い方
ローカルで試す
```
mvn package
java -jar target/logucene-1.0-SNAPSHOT-jar-with-dependencies.jar
```
Dockerで試す
```
sudo docker run --rm -v $PWD:/workdir --workdir /workdir --user `id -u`:`id -g` maven mvn package
sudo docker run --rm \
  --user `id -u`:`id -g` \
  --workdir /opt/logucene \
  -v $PWD/target:/opt/logucene \
  -p 1514:1514/udp \
  -p 8080:8080 \
  openjdk:21 \
  java -Duser.timezone=Asia/Tokyo -jar logucene-1.0-SNAPSHOT-jar-with-dependencies.jar
```

# オプション
```
java \
  -Dsyslog.port=1514 \
  -Dweb.port=8080 \
  -Dlucene.index=index \
  -Dlucene.analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer \
  -Duser.timezone=Asia/Tokyo \
  -Dsyslog.timezone=Asia/Tokyo \
  -Dsyslog.timezone[127.0.0.1]=Asia/Tokyo \
  -Dsyslog.listener=/path/to/script.groovy \
  -jar target/logucene-1.0-SNAPSHOT-jar-with-dependencies.jar
```
| Key                               | Value                                                                             | Default                                              |
| --------------------------------- | --------------------------------------------------------------------------------- | ---------------------------------------------------- |
| syslog.port                       | syslogの受信ポート(UDP)                                                           | 1514                                                 |
| web.port                          | webサーバの待受ポート                                                             | 8080                                                 |
| lucene.index                      | luceneの保存先ディレクトリ                                                        | index                                                |
| lucene.analyzer                   | 全文検索に使用するアナライザ                                                      | org.apache.lucene.analysis.standard.StandardAnalyzer |
| user.timezone                     | ブラウザで日時を表示する際に使用するタイムゾーン                                  | システムのタイムゾーン                               |
| syslog.timezone                   | RFC3164フォーマットのログに含まれる日時をパースする際に使用するタイムゾーン(共通) | システムのタイムゾーン                               |
| syslog.timezone[送信元IPアドレス] | RFC3164フォーマットのログに含まれる日時をパースする際に使用するタイムゾーン(個別) | システムのタイムゾーン                               |
| syslog.listener                   | ログ受信時に実行したいGroovyスクリプトのファイルパス                              | (無し)                                               |
