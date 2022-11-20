import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class LogServer {

    public static void main(String[] args) throws IOException {
        new LogServer().start();
    }

    private void start() throws IOException {
        final InetSocketAddress serverAddress = new InetSocketAddress("0.0.0.0", 7070);
        HttpServer localhost = HttpServer.create(serverAddress, 100);
        localhost.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        localhost.createContext("/", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                String logLevel = exchange.getRequestHeaders().get("X-A-LogLevel").get(0);
                String environment = exchange.getRequestHeaders().get("X-A-Environment").get(0);
                String tenantId = exchange.getRequestHeaders().get("X-A-TenantId").get(0);
                String logToken = exchange.getRequestHeaders().get("X-A-LogToken").get(0);
                String requestBody = new String(exchange.getRequestBody().readAllBytes());
                log("%s,%s,%s,%s %s".formatted(logLevel, environment, tenantId, logToken, requestBody)); //   \[.*?\]
                exchange.sendResponseHeaders(201, 0);
            }

            private void log(String body) {
                System.out.println(body);
            }
        });
        localhost.start();
    }
}
