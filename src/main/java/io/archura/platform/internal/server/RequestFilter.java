package io.archura.platform.internal.server;

import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.HttpExchange;
import io.archura.platform.api.http.HttpStatusCode;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

@Slf4j
public class RequestFilter extends Filter {

    private final Integer requestTimeout;

    public RequestFilter(final int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    @Override
    public void doFilter(HttpExchange exchange, Chain chain) throws IOException {
        final Thread currentThread = Thread.currentThread();
        final Timer timer = new Timer();
        final LocalDateTime time = LocalDateTime.now().plusSeconds(requestTimeout);
        final Date date = Date.from(time.atZone(ZoneId.systemDefault()).toInstant());
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (currentThread.isVirtual() && currentThread.isAlive()) {
                    try {
                        exchange.sendResponseHeaders(HttpStatusCode.HTTP_GATEWAY_TIMEOUT, 0);
                        exchange.getResponseBody().close();
                    } catch (Exception e) {
                        log.error("Timeout response error: {}", e.getMessage());
                    }
                    currentThread.interrupt();
                }
            }
        }, date);

        chain.doFilter(exchange);

        timer.cancel();
    }

    @Override
    public String description() {
        return "Time out filter.";
    }
}