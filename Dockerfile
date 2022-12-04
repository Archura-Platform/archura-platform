FROM openjdk:19-slim

ARG JAVA_OPTS

ENV APP_NAME=ArchuraPlatformApplication
ARG APP_HOME=/opt/app

RUN groupadd app && useradd -d ${APP_HOME} -s /bin/nologin -g app app
WORKDIR ${APP_HOME}
COPY target/ArchuraPlatformApplication-0.0.1-SNAPSHOT.jar ${APP_NAME}.jar
EXPOSE 8080

USER app
ENTRYPOINT echo "Starting container "`date` && java -XX:MaxRAMPercentage=50 -XX:MinRAMPercentage=50 \
-Djava.security.manager=io.archura.platform.securitymanager.ArchuraSecurityManager \
--enable-preview --add-exports java.base/jdk.internal.reflect=ALL-UNNAMED \
--add-opens java.base/java.security=ALL-UNNAMED \
--add-opens java.base/java.lang=ALL-UNNAMED \
${JAVA_OPTS} -jar ${APP_NAME}.jar
