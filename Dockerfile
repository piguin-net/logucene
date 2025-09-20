FROM docker.io/library/openjdk:21
ARG VERSION=1.3

WORKDIR /opt/logucene
VOLUME ["/opt/logucene/index"]
COPY target/logucene-${VERSION}-SNAPSHOT-jar-with-dependencies.jar /opt/logucene/logucene.jar

ENV LUCENE_INDEX=index
ENV LUCENE_ANALYZER=org.apache.lucene.analysis.cjk.CJKAnalyzer
ENV SQLITE_ANALYZER=org.apache.lucene.analysis.cjk.CJKAnalyzer
ENV SYSTEM_TIMEZONE=Asia/Tokyo
ENV SYSLOG_TIMEZONE=Asia/Tokyo
ENV SYSLOG_LISTENER=

EXPOSE 2514/udp 8080

CMD ["java", "-jar", "/opt/logucene/logucene.jar"]
