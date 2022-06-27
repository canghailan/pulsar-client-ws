package cc.whohow.pulsar.client.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.transaction.TransactionBuilder;
import org.apache.pulsar.shade.org.apache.commons.io.input.CharSequenceReader;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

public class WebSocketPulsarClient implements PulsarClient {
    protected final Collection<Producer<?>> producers = new CopyOnWriteArrayList<>();
    protected final Collection<Consumer<?>> consumers = new CopyOnWriteArrayList<>();
    protected final Collection<Reader<?>> readers = new CopyOnWriteArrayList<>();
    protected final HttpClient httpClient;
    protected final ObjectMapper json;
    protected volatile String serviceUrl;
    protected volatile WebSocket.Builder webSocketBuilder;

    public WebSocketPulsarClient(HttpClient httpClient, ObjectMapper json, String serviceUrl) {
        this.httpClient = httpClient;
        this.json = json;
        this.serviceUrl = serviceUrl;
        this.webSocketBuilder = httpClient.newWebSocketBuilder();
    }

    public CompletableFuture<WebSocket> connectAsync(URI uri, WebSocket.Listener listener) {
        if (isClosed()) {
            return CompletableFuture.failedFuture(new PulsarClientException.AlreadyClosedException("closed"));
        }
        return webSocketBuilder.buildAsync(uri, listener);
    }

    public ObjectNode newJsonObject() {
        return json.createObjectNode();
    }

    public String stringify(JsonNode value) throws PulsarClientException {
        try {
            return json.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new PulsarClientException(e);
        }
    }

    public JsonNode parse(CharSequence text) throws PulsarClientException {
        try (CharSequenceReader reader = new CharSequenceReader(text)) {
            return json.readTree(reader);
        } catch (IOException e) {
            throw new PulsarClientException(e);
        }
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    @Override
    public ProducerBuilder<byte[]> newProducer() {
        return newProducer(Schema.BYTES);
    }

    @Override
    public <T> ProducerBuilder<T> newProducer(Schema<T> schema) {
        return new WebSocketProducerBuilder<>(this, schema);
    }

    public <T> ProducerBuilder<T> newProducer(Class<T> type) {
        return newProducer(new JsonSchema<>(json, type));
    }

    public <T> ProducerBuilder<T> newProducer(TypeReference<T> type) {
        return newProducer(new JsonSchema<>(json, type));
    }

    @Override
    public ConsumerBuilder<byte[]> newConsumer() {
        return newConsumer(Schema.BYTES);
    }

    @Override
    public <T> ConsumerBuilder<T> newConsumer(Schema<T> schema) {
        return new WebSocketConsumerBuilder<>(this, schema);
    }

    public <T> ConsumerBuilder<T> newConsumer(Class<T> type) {
        return newConsumer(new JsonSchema<>(json, type));
    }

    public <T> ConsumerBuilder<T> newConsumer(TypeReference<T> type) {
        return newConsumer(new JsonSchema<>(json, type));
    }

    @Override
    public ReaderBuilder<byte[]> newReader() {
        return newReader(Schema.BYTES);
    }

    @Override
    public <T> ReaderBuilder<T> newReader(Schema<T> schema) {
        return new WebSocketReaderBuilder<>(this, schema);
    }

    public <T> ReaderBuilder<T> newReader(Class<T> type) {
        return newReader(new JsonSchema<>(json, type));
    }

    public <T> ReaderBuilder<T> newReader(TypeReference<T> type) {
        return newReader(new JsonSchema<>(json, type));
    }

    @Override
    public void updateServiceUrl(String serviceUrl) throws PulsarClientException {
        if (this.serviceUrl == null) {
            this.serviceUrl = serviceUrl;
        } else {
            throw PulsarWebSocket.unsupported("updateServiceUrl");
        }
    }

    @Override
    public CompletableFuture<List<String>> getPartitionsForTopic(String topic) {
        return CompletableFuture.completedFuture(Collections.singletonList(topic));
    }

    @Override
    public void close() throws PulsarClientException {
        PulsarWebSocket.await(closeAsync());
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        webSocketBuilder = null;
        return CompletableFuture.allOf(
                CompletableFuture.allOf(producers.stream().map(this::close).toArray(CompletableFuture[]::new))
                        .thenAccept((v) -> producers.clear()),
                CompletableFuture.allOf(consumers.stream().map(this::close).toArray(CompletableFuture[]::new))
                        .thenAccept((v) -> consumers.clear()),
                CompletableFuture.allOf(readers.stream().map(this::close).toArray(CompletableFuture[]::new))
                        .thenAccept((v) -> readers.clear())
        );
    }

    protected CompletableFuture<?> close(Producer<?> producer) {
        return producer.flushAsync().thenCompose((v) -> producer.closeAsync());
    }

    protected CompletableFuture<?> close(Consumer<?> consumer) {
        consumer.pause();
        return consumer.closeAsync();
    }

    protected CompletableFuture<?> close(Reader<?> reader) {
        return reader.closeAsync();
    }

    @Override
    public void shutdown() throws PulsarClientException {
        webSocketBuilder = null;
        PulsarWebSocket.await(CompletableFuture.allOf(
                CompletableFuture.allOf(producers.stream().map(this::shutdown).toArray(CompletableFuture[]::new))
                        .thenAccept((v) -> producers.clear()),
                CompletableFuture.allOf(consumers.stream().map(this::shutdown).toArray(CompletableFuture[]::new))
                        .thenAccept((v) -> consumers.clear()),
                CompletableFuture.allOf(readers.stream().map(this::shutdown).toArray(CompletableFuture[]::new))
                        .thenAccept((v) -> readers.clear())
        ));
    }

    protected CompletableFuture<?> shutdown(Producer<?> producer) {
        return producer.closeAsync();
    }

    protected CompletableFuture<?> shutdown(Consumer<?> consumer) {
        return consumer.closeAsync();
    }

    protected CompletableFuture<?> shutdown(Reader<?> reader) {
        return reader.closeAsync();
    }

    @Override
    public boolean isClosed() {
        return webSocketBuilder == null;
    }

    @Override
    public TransactionBuilder newTransaction() {
        throw PulsarWebSocket.unsupportedUnchecked("Transaction");
    }
}
