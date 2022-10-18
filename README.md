# Archura Platform
Archura Platform.

Add the following VM parameter to enable sandboxing.

```
-Djava.security.manager=io.archura.platform.securitymanager.ArchuraSecurityManager --enable-preview --add-exports java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens java.base/java.security=ALL-UNNAMED  --add-opens java.base/java.lang=ALL-UNNAMED  
```

Also limit the file system availability.

```
# create a docker volume
docker volume create temp1m --driver local --opt type=tmpfs --opt device=tmpfs --opt o=size=1m,uid=1000

# run the docker container
docker run --rm -it -v temp1m:/tmp/  \
--read-only \
--memory="256MB" \
--cpus="2.0" \
-p 8080:8080 \
-e CONFIG_REPOSITORY_URL='http://IP-OF-THE-CONFIG-REPOSITORY:9090/gateway/v1' \
--name archura-platform archura-platform-app:0.0.1
```