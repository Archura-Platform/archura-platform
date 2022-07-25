FROM openjdk:19-slim

ENV APP_NAME=ArchuraPlatformApplication
ARG APP_HOME=/opt/app

RUN groupadd app && useradd -d ${APP_HOME} -s /bin/nologin -g app app
WORKDIR ${APP_HOME}
COPY target/archura-platform-0.0.1-SNAPSHOT.jar ${APP_NAME}.jar
EXPOSE 8080

USER app
ENTRYPOINT java -XX:MaxRAMPercentage=100 -XX:MinRAMPercentage=100 \
--enable-preview --add-exports java.base/jdk.internal.reflect=ALL-UNNAMED \
-Ddebug -noverify -Dspring.output.ansi.enabled=always \
-jar ${APP_NAME}.jar
