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
mkdir index
sudo docker run --rm -v $PWD:/workdir --workdir /workdir --user `id -u`:`id -g` maven mvn package
sudo docker run --rm \
  --user `id -u`:`id -g` \
  --workdir /opt/logucene \
  -v $PWD/target:/opt/logucene \
  -p 514:514/udp \
  -p 8080:8080 \
  openjdk:21 \
  java -Dsystem.timezone=Asia/Tokyo -jar logucene-1.0-SNAPSHOT-jar-with-dependencies.jar
```
Dockerのイメージを作成して試す
```
mkdir index
sudo docker run --rm -v $PWD:/workdir --workdir /workdir --user `id -u`:`id -g` maven mvn package
sudo docker build -t logucene --build-arg VERSION=1.0 .
sudo docker run -it -d \
  --name logucene \
  --restart=always \
  --user `id -u`:`id -g` \
  -v $PWD/index:/opt/logucene/index \
  -p 514:514/udp \
  -p 8080:8080 \
  logucene
```

# オプション
```
java \
  -Dsyslog.port=514 \
  -Dweb.port=8080 \
  -Dlucene.index=index \
  -Dlucene.analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer \
  -Dsqlite.analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer \
  -Dsystem.timezone=Asia/Tokyo \
  -Dsyslog.timezone=Asia/Tokyo \
  -Dsyslog.timezone[127.0.0.1]=Asia/Tokyo \
  -Dsyslog.listener=/path/to/script.groovy \
  -jar target/logucene-1.0-SNAPSHOT-jar-with-dependencies.jar
```
| SystemProperty                    | 環境変数                             | Value                                                                             | Default                                              |
| --------------------------------- | ------------------------------------ | --------------------------------------------------------------------------------- | ---------------------------------------------------- |
| syslog.port                       | SYSLOG_PORT                          | syslogの受信ポート(UDP)                                                           | 1514                                                 |
| web.port                          | WEB_PORT                             | webサーバの待受ポート                                                             | 8080                                                 |
| lucene.index                      | LUCENE_INDEX                         | luceneの保存先ディレクトリ                                                        | index                                                |
| lucene.analyzer                   | LUCENE_ANALYZER                      | luceneの全文検索に使用するアナライザ                                              | org.apache.lucene.analysis.standard.StandardAnalyzer |
| sqlite.analyzer                   | SQLITE_ANALYZER                      | SQLiteファイルダウンロード時に使用するアナライザ(トークナイザ)                    | org.apache.lucene.analysis.standard.StandardAnalyzer |
| system.timezone                   | SYSTEM_TIMEZONE                      | ブラウザで日時を表示する際に使用するタイムゾーン                                  | System.getProperty("user.timezone")                  |
| syslog.timezone                   | SYSLOG_TIMEZONE                      | RFC3164フォーマットのログに含まれる日時をパースする際に使用するタイムゾーン(共通) | system.timezoneの値                                  |
| syslog.timezone[送信元IPアドレス] | SYSLOG_TIMEZONE_送信元IPアドレス(※) | RFC3164フォーマットのログに含まれる日時をパースする際に使用するタイムゾーン(個別) | syslog.timezoneの値                                  |
| syslog.listener                   | SYSLOG_LISTENER                      | ログ受信時に実行したいGroovyスクリプトのファイルパス                              | (無し)                                               |
※環境変数の送信元IPアドレスは"."(IPv4)及び":"(IPv6)を"_"へ置換して指定する