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
  java -jar logucene-1.0-SNAPSHOT-jar-with-dependencies.jar
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
