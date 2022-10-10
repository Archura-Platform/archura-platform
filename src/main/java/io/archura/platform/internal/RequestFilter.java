package io.archura.platform.internal;

import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public class RequestFilter extends Filter {

    private Integer requestTimeout = 30;

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