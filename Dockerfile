FROM openjdk:21
ARG VERSION=1.0

WORKDIR /opt/logucene
VOLUME ["/opt/logucene/index"]
COPY target/logucene-${VERSION}-SNAPSHOT-jar-with-dependencies.jar /opt/logucene/logucene.jar

ENV LUCENE_INDEX=index
ENV LUCENE_ANALYZER=org.apache.lucene.analysis.standard.StandardAnalyzer
ENV SQLITE_ANALYZER=org.apache.lucene.analysis.standard.StandardAnalyzer
ENV SYSTEM_TIMEZONE=Asia/Tokyo
ENV SYSLOG_TIMEZONE=Asia/Tokyo
ENV SYSLOG_LISTENER=

EXPOSE 514/udp 8080

CMD ["java", "-jar", "/opt/logucene/logucene.jar"]
