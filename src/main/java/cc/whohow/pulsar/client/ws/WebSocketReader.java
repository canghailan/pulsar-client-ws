package cc.whohow.pulsar.client.ws;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pulsar.client.api.*;

import java.net.http.WebSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public abstract class WebSocketReader<T> extends PulsarWebSocket implements Reader<T>, WebSocket.Listener {
    protected final WebSocketReaderEndpoint endpoint;
    protected final Schema<T> schema;
    protected final ReceiveQueue.S<Boolean> endOfTopic = new ReceiveQueue.S<>();

    protected WebSocketReader(WebSocketPulsarClient client, WebSocketReaderEndpoint endpoint, Schema<T> schema) {
        super(client);
        this.endpoint = endpoint;
        this.schema = schema;
    }

    @Override
    protected String getEndpointUrl() {
        return endpoint.toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Reader<T>> connectAsync() {
        return (CompletableFuture<Reader<T>>) super.connectAsync();
    }


    @Override
    public String getTopic() {
        return endpoint.getTopic();
    }

    @Override
    public Message<T> readNext() throws PulsarClientException {
        return await(readNextAsync());
    }

    @Override
    public Message<T> readNext(int timeout, TimeUnit unit) throws PulsarClientException {
        return await(readNextAsync(), timeout, unit);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return super.closeAsync().thenAccept((v) -> {
            endOfTopic.clear();
        });
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return awaitUnchecked(isEndOfTopic().thenCompose((v) -> endOfTopic.receiveAsync()));
    }

    @Override
    public boolean hasMessageAvailable() throws PulsarClientException {
        return await(hasMessageAvailableAsync());
    }

    @Override
    public void seek(MessageId messageId) throws PulsarClientException {
        await(seekAsync(messageId));
    }

    @Override
    public void seek(long timestamp) throws PulsarClientException {
        await(seekAsync(timestamp));
    }

    @Override
    public CompletableFuture<Void> seekAsync(MessageId messageId) {
        return seekAsync(toJsonMessageId(messageId));
    }

    @Override
    public CompletableFuture<Void> seekAsync(long timestamp) {
        return seekAsync(toJsonMessageId(timestamp));
    }

    protected CompletableFuture<Void> seekAsync(String messageId) {
        return closeAsync().thenCompose((v) -> {
            endpoint.setMessageId(messageId);
            return connectAsync().thenAccept((c) -> {
            });
        });
    }

    protected CompletableFuture<?> acknowledge(String messageId) {
        return sendJson(newJsonObject()
                .put(JsonMessage.MESSAGE_ID, messageId));
    }

    protected CompletableFuture<?> isEndOfTopic() {
        return sendJson(newJsonObject()
                .put(TYPE, IS_END_OF_TOPIC));
    }

    @Override
    protected void onJson(JsonNode message) {
        String messageId = message.path(JsonMessage.MESSAGE_ID).textValue();
        if (messageId != null) {
            onMessage(new JsonMessage<>(getTopic(), schema, message));
            acknowledge(messageId);
        } else {
            JsonNode endOfTopic = message.get(PulsarWebSocket.END_OF_TOPIC);
            if (endOfTopic != null) {
                onEndOfTopic(endOfTopic.booleanValue());
            }
        }
    }

    protected abstract void onMessage(Message<T> message);

    protected void onEndOfTopic(boolean endOfTopic) {
        this.endOfTopic.received(endOfTopic);
    }

    public static class Queue<T> extends WebSocketReader<T> {
        protected final ReceiveQueue.S<Message<T>> message = new ReceiveQueue.S<>();

        protected Queue(WebSocketPulsarClient client, WebSocketReaderEndpoint endpoint, Schema<T> schema) {
            super(client, endpoint, schema);
        }

        @Override
        public CompletableFuture<Message<T>> readNextAsync() {
            return message.receiveAsync();
        }

        @Override
        public boolean hasMessageAvailable() throws PulsarClientException {
            return message.available();
        }

        @Override
        public CompletableFuture<Boolean> hasMessageAvailableAsync() {
            return CompletableFuture.completedFuture(message.available());
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return super.closeAsync().thenAccept((v) -> {
                message.clear();
            });
        }

        @Override
        protected void onMessage(Message<T> message) {
            this.message.received(message);
        }
    }

    public static class Listener<T> extends WebSocketReader<T> {
        protected final ReaderListener<T> listener;

        protected Listener(WebSocketPulsarClient client, WebSocketReaderEndpoint endpoint, Schema<T> schema,
                           ReaderListener<T> listener) {
            super(client, endpoint, schema);
            this.listener = listener;
        }

        @Override
        public CompletableFuture<Message<T>> readNextAsync() {
            return CompletableFuture.failedFuture(new PulsarClientException.NotAllowedException("read"));
        }

        @Override
        public CompletableFuture<Boolean> hasMessageAvailableAsync() {
            return CompletableFuture.failedFuture(new PulsarClientException.NotAllowedException("hasMessageAvailable"));
        }

        @Override
        protected void onMessage(Message<T> message) {
            listener.received(this, message);
        }

        @Override
        protected void onEndOfTopic(boolean endOfTopic) {
            super.onEndOfTopic(endOfTopic);
            if (endOfTopic) {
                listener.reachedEndOfTopic(this);
            }
        }
    }
}
