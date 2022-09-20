package io.archura.platform.internal;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.nonNull;

public class RequestInterceptor implements HandlerInterceptor {

    @Value("${server.request.timeout:60}")
    private Integer requestTimeout;
    private final Map<Thread, Timer> threads = new ConcurrentHashMap<>();

    @Override
    public boolean preHandle(
            HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        final Thread currentThread = Thread.currentThread();
        final Timer timer = new Timer();
        threads.put(currentThread, timer);

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
        return true;
    }

    @Override
    public void postHandle(
            HttpServletRequest request, HttpServletResponse response, Object handler,
            ModelAndView modelAndView) throws Exception {
        final Thread currentThread = Thread.currentThread();
        final Timer timer = threads.remove(currentThread);
        if (nonNull(timer)) {
            timer.cancel();
        }
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response,
                                Object handler, Exception exception) throws Exception {
    }
}