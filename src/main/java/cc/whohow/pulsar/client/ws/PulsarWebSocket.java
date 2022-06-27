package cc.whohow.pulsar.client.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.WebSocket;
import java.util.concurrent.*;

public abstract class PulsarWebSocket implements WebSocket.Listener {
    public static final String TYPE = "type";
    public static final String NEGATIVE_ACKNOWLEDGE = "negativeAcknowledge";
    public static final String PERMIT = "permit";
    public static final String PERMIT_MESSAGES = "permitMessages";
    public static final String IS_END_OF_TOPIC = "isEndOfTopic";
    public static final String END_OF_TOPIC = "endOfTopic";
    public static final int ABNORMAL_CLOSURE = 1006;
    private static final Logger LOG = LoggerFactory.getLogger(PulsarWebSocket.class);
    protected final WebSocketPulsarClient client;
    protected volatile WebSocket webSocket;
    protected volatile StringBuilder buffer = new StringBuilder(0);

    public PulsarWebSocket(WebSocketPulsarClient client) {
        this.client = client;
    }

    public static <T> T await(CompletableFuture<T> future) throws PulsarClientException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new PulsarClientException(e);
        } catch (ExecutionException e) {
            throw new PulsarClientException(e.getCause());
        }
    }

    public static <T> T await(CompletableFuture<T> future, int timeout, TimeUnit unit) throws PulsarClientException {
        try {
            return future.get(timeout, unit);
        } catch (InterruptedException | TimeoutException e) {
            throw new PulsarClientException(e);
        } catch (ExecutionException e) {
            throw new PulsarClientException(e.getCause());
        }
    }

    public static <T> T awaitUnchecked(CompletableFuture<T> future) {
        return future.join();
    }

    public static String toJsonMessageId(MessageId messageId) {
        return messageId.toString();
    }

    public static String toJsonMessageId(long timestamp) {
        return Long.toString(timestamp);
    }

    public static PulsarClientException unsupported(String message) {
        return new PulsarClientException.NotSupportedException(message);
    }

    public static UnsupportedOperationException unsupportedUnchecked(String message) {
        return new UnsupportedOperationException(unsupported(message));
    }

    public static <T> CompletableFuture<T> unsupportedAsync(String message) {
        return CompletableFuture.failedFuture(unsupported(message));
    }

    protected abstract String getEndpointUrl();

    public CompletableFuture<?> connectAsync() {
        URI uri = URI.create(getEndpointUrl());
        LOG.debug("Connect {}", uri);
        return client.connectAsync(uri, this).thenApply((ws) -> {
            LOG.debug("Connected {}", ws);
            this.webSocket = ws;
            return this;
        });
    }

    public boolean isConnected() {
        return webSocket != null;
    }

    public void close() throws PulsarClientException {
        await(closeAsync());
    }

    public CompletableFuture<Void> closeAsync() {
        LOG.debug("Close {}", webSocket);
        return close(WebSocket.NORMAL_CLOSURE, "close")
                .thenAccept((v) -> {
                    LOG.debug("Closed {}", webSocket);
                    webSocket = null;
                });
    }

    protected CompletableFuture<?> close(int statusCode, String reason) {
        LOG.trace("close {} {}", statusCode, reason);
        if (webSocket == null) {
            return CompletableFuture.completedFuture(null);
        } else {
            return webSocket.sendClose(statusCode, reason);
        }
    }

    protected ObjectNode newJsonObject() {
        return client.newJsonObject();
    }

    protected CompletableFuture<?> sendJson(JsonNode value) {
        LOG.trace("<- {}", value);
        try {
            return webSocket.sendText(client.stringify(value), true);
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        try {
            if (last) {
                if (buffer.length() == 0) {
                    JsonNode message = client.parse(data);
                    LOG.trace("-> {}", message);
                    onJson(message);
                } else {
                    buffer.append(data);
                    JsonNode message = client.parse(buffer);
                    LOG.trace("-> {}", message);
                    onJson(message);
                    buffer.setLength(0);
                }
            } else {
                buffer.append(data);
            }
        } catch (Throwable e) {
            close(PulsarWebSocket.ABNORMAL_CLOSURE, e.getMessage());
        }
        return WebSocket.Listener.super.onText(webSocket, data, last);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        LOG.warn("onError", error);
        WebSocket.Listener.super.onError(webSocket, error);
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        LOG.trace("onClose {} {}", statusCode, reason);
        this.webSocket = null;
        return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
    }

    protected abstract void onJson(JsonNode message) throws Exception;
}
