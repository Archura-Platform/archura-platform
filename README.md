# Archura Platform
Archura Platform.

Add the following VM parameter to enable sandboxing.

```
-Djava.security.manager=io.archura.platform.internal.security.ThreadSecurityManager --enable-preview --add-exports java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens java.base/java.security=ALL-UNNAMED  --add-opens java.base/java.lang=ALL-UNNAMED  
```
