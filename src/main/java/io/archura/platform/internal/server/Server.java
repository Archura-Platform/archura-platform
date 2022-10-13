package io.archura.platform.internal.server;

import io.archura.platform.internal.Assets;
import io.archura.platform.internal.RequestHandler;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public interface Server {

    void start(RequestHandler requestHandler, ExecutorService executorService, Assets assets) throws IOException;

    void stop();
}
