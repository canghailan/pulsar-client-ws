package cc.whohow.pulsar.client.ws;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.transaction.Transaction;

import java.net.http.WebSocket;
import java.util.concurrent.CompletableFuture;

public class WebSocketProducer<T> extends PulsarWebSocket implements Producer<T>, WebSocket.Listener {
    protected final WebSocketProducerEndpoint endpoint;
    protected final Schema<T> schema;
    protected final ReceiveQueue.Ex<MessageId> messageId = new ReceiveQueue.Ex<>();

    public WebSocketProducer(WebSocketPulsarClient client, WebSocketProducerEndpoint endpoint, Schema<T> schema) {
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
    public CompletableFuture<Producer<T>> connectAsync() {
        return (CompletableFuture<Producer<T>>) super.connectAsync();
    }


    @Override
    public String getTopic() {
        return endpoint.getTopic();
    }

    @Override
    public String getProducerName() {
        return endpoint.getProducerName();
    }

    @Override
    public MessageId send(T message) throws PulsarClientException {
        return await(sendAsync(message));
    }

    @Override
    public CompletableFuture<MessageId> sendAsync(T message) {
        return sendAsync(newJsonObject()
                .put(JsonMessage.PAYLOAD, schema.encode(message)));
    }

    public MessageId send(JsonNode message) throws PulsarClientException {
        return await(sendAsync(message));
    }

    public CompletableFuture<MessageId> sendAsync(JsonNode message) {
        return publish(message).thenCompose((v) -> messageId.receiveAsync());
    }

    @Override
    public void flush() throws PulsarClientException {
        await(flushAsync());
    }

    @Override
    public CompletableFuture<Void> flushAsync() {
        return messageId.all();
    }

    @Override
    public TypedMessageBuilder<T> newMessage() {
        return newMessage(schema);
    }

    @Override
    public <V> TypedMessageBuilder<V> newMessage(Schema<V> schema) {
        return new WebSocketTypedMessageBuilder<>(this, schema, newJsonObject());
    }

    @Override
    public TypedMessageBuilder<T> newMessage(Transaction txn) {
        throw unsupportedUnchecked("Transaction");
    }

    @Override
    public long getLastSequenceId() {
        return 0;
    }

    @Override
    public ProducerStats getStats() {
        return null;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return super.closeAsync().thenAccept((v) -> {
            this.messageId.clear();
        });
    }

    @Override
    public long getLastDisconnectedTimestamp() {
        return 0;
    }

    public CompletableFuture<?> publish(JsonNode message) {
        return sendJson(message);
    }

    @Override
    protected void onJson(JsonNode message) {
        if ("ok".equals(message.path("result").textValue())) {
            String messageId = message.path("messageId").textValue();
            onSuccess(new JsonMessage.Id(messageId));
        } else {
            String errorMsg = message.path("errorMsg").textValue();
            onFailure(new PulsarClientException(errorMsg));
        }
    }

    protected void onSuccess(MessageId messageId) {
        this.messageId.received(messageId);
    }

    protected void onFailure(PulsarClientException e) {
        this.messageId.received(e);
    }
}
