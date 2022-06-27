package cc.whohow.pulsar.client.ws;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.transaction.Transaction;

import java.net.http.WebSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public abstract class WebSocketConsumer<T> extends PulsarWebSocket implements Consumer<T>, WebSocket.Listener {
    protected final LongAdder totalMsgsReceived = new LongAdder();
    protected final LongAdder totalAcksSent = new LongAdder();
    protected final WebSocketConsumerEndpoint endpoint;
    protected final Schema<T> schema;
    protected final ReceiveQueue.S<Boolean> endOfTopic = new ReceiveQueue.S<>();
    protected volatile MessageId lastMessageId;
    protected volatile boolean paused = false;

    protected WebSocketConsumer(WebSocketPulsarClient client, WebSocketConsumerEndpoint endpoint, Schema<T> schema) {
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
    public CompletableFuture<Consumer<T>> connectAsync() {
        return (CompletableFuture<Consumer<T>>) super.connectAsync();
    }

    @Override
    public String getTopic() {
        return endpoint.getTopic();
    }

    @Override
    public String getSubscription() {
        return endpoint.getSubscription();
    }

    @Override
    public void unsubscribe() throws PulsarClientException {
        await(unsubscribeAsync());
    }

    @Override
    public CompletableFuture<Void> unsubscribeAsync() {
        return unsupportedAsync("unsubscribe");
    }

    @Override
    public Message<T> receive() throws PulsarClientException {
        return await(receiveAsync());
    }

    @Override
    public Message<T> receive(int timeout, TimeUnit unit) throws PulsarClientException {
        return await(receiveAsync(), timeout, unit);
    }

    @Override
    public Messages<T> batchReceive() throws PulsarClientException {
        return await(batchReceiveAsync());
    }

    @Override
    public CompletableFuture<Messages<T>> batchReceiveAsync() {
        return unsupportedAsync("batchReceive");
    }

    @Override
    public void acknowledge(Message<?> message) throws PulsarClientException {
        acknowledge(message.getMessageId());
    }

    @Override
    public void acknowledge(MessageId messageId) throws PulsarClientException {
        await(acknowledgeAsync(messageId));
    }

    @Override
    public void acknowledge(Messages<?> messages) throws PulsarClientException {
        acknowledge(getMessageIdList(messages));
    }

    @Override
    public void acknowledge(List<MessageId> messageIdList) throws PulsarClientException {
        await(acknowledgeAsync(messageIdList));
    }

    @Override
    public void negativeAcknowledge(Message<?> message) {
        negativeAcknowledge(message.getMessageId());
    }

    @Override
    public void negativeAcknowledge(MessageId messageId) {
        awaitUnchecked(negativeAcknowledge(toJsonMessageId(messageId)));
    }

    @Override
    public void negativeAcknowledge(Messages<?> messages) {
        for (Message<?> message : messages) {
            negativeAcknowledge(message);
        }
    }

    @Override
    public void reconsumeLater(Message<?> message, long delayTime, TimeUnit unit) throws PulsarClientException {
        await(reconsumeLaterAsync(message, delayTime, unit));
    }

    @Override
    public void reconsumeLater(Messages<?> messages, long delayTime, TimeUnit unit) throws PulsarClientException {
        await(reconsumeLaterAsync(messages, delayTime, unit));
    }

    @Override
    public void acknowledgeCumulative(Message<?> message) throws PulsarClientException {
        acknowledgeCumulative(message.getMessageId());
    }

    @Override
    public void acknowledgeCumulative(MessageId messageId) throws PulsarClientException {
        await(acknowledgeCumulativeAsync(messageId));
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId, Transaction txn) {
        return unsupportedAsync("Transaction");
    }

    @Override
    public void reconsumeLaterCumulative(Message<?> message, long delayTime, TimeUnit unit) throws PulsarClientException {
        await(reconsumeLaterCumulativeAsync(message, delayTime, unit));
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(Message<?> message) {
        return acknowledgeAsync(message.getMessageId());
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(MessageId messageId) {
        return acknowledge(toJsonMessageId(messageId)).thenAccept((v) -> {
        });
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(MessageId messageId, Transaction txn) {
        return unsupportedAsync("Transaction");
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(Messages<?> messages) {
        return acknowledgeAsync(getMessageIdList(messages));
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(List<MessageId> messageIdList) {
        return CompletableFuture.allOf(messageIdList.stream()
                .map(this::acknowledgeAsync)
                .toArray(CompletableFuture[]::new));
    }

    @Override
    public CompletableFuture<Void> reconsumeLaterAsync(Message<?> message, long delayTime, TimeUnit unit) {
        return unsupportedAsync("reconsumeLater");
    }

    @Override
    public CompletableFuture<Void> reconsumeLaterAsync(Messages<?> messages, long delayTime, TimeUnit unit) {
        return unsupportedAsync("reconsumeLater");
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(Message<?> message) {
        return acknowledgeCumulativeAsync(message.getMessageId());
    }

    @Override
    public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId) {
        return unsupportedAsync("acknowledgeCumulative");
    }

    @Override
    public CompletableFuture<Void> reconsumeLaterCumulativeAsync(Message<?> message, long delayTime, TimeUnit unit) {
        return unsupportedAsync("reconsumeLaterCumulative");
    }

    @Override
    public ConsumerStats getStats() {
        return null;
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
    public void redeliverUnacknowledgedMessages() {
        // ignore
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
        return unsupportedAsync("seek");
    }

    @Override
    public CompletableFuture<Void> seekAsync(long timestamp) {
        return unsupportedAsync("seek");
    }

    @Override
    public MessageId getLastMessageId() throws PulsarClientException {
        return lastMessageId;
    }

    @Override
    public CompletableFuture<MessageId> getLastMessageIdAsync() {
        return null;
    }

    @Override
    public String getConsumerName() {
        return endpoint.getConsumerName();
    }

    @Override
    public void pause() {
        paused = true;
    }

    @Override
    public void resume() {
        paused = false;
    }

    @Override
    public long getLastDisconnectedTimestamp() {
        return 0;
    }

    protected List<MessageId> getMessageIdList(Messages<?> messages) {
        List<MessageId> messageIdList = new ArrayList<>(messages.size());
        for (Message<?> message : messages) {
            messageIdList.add(message.getMessageId());
        }
        return messageIdList;
    }

    protected CompletableFuture<?> acknowledge(String messageId) {
        totalAcksSent.increment();
        return sendJson(newJsonObject()
                .put(JsonMessage.MESSAGE_ID, messageId));
    }

    protected CompletableFuture<?> negativeAcknowledge(String messageId) {
        return sendJson(newJsonObject()
                .put(PulsarWebSocket.TYPE, PulsarWebSocket.NEGATIVE_ACKNOWLEDGE)
                .put(JsonMessage.MESSAGE_ID, messageId));
    }

    protected CompletableFuture<?> permit(int permitMessages) {
        return sendJson(newJsonObject()
                .put(PulsarWebSocket.TYPE, PulsarWebSocket.PERMIT)
                .put(PulsarWebSocket.PERMIT_MESSAGES, permitMessages));
    }

    protected CompletableFuture<?> isEndOfTopic() {
        return sendJson(newJsonObject()
                .put(TYPE, IS_END_OF_TOPIC));
    }

    @Override
    protected void onJson(JsonNode message) {
        String messageId = message.path(JsonMessage.MESSAGE_ID).textValue();
        if (messageId != null) {
            totalMsgsReceived.increment();
            onMessage(new JsonMessage<>(getTopic(), schema, message));
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

    public static class Queue<T> extends WebSocketConsumer<T> {
        protected final ReceiveQueue.S<Message<T>> message = new ReceiveQueue.S<>();

        protected Queue(WebSocketPulsarClient client, WebSocketConsumerEndpoint endpoint, Schema<T> schema) {
            super(client, endpoint, schema);
        }

        @Override
        protected void onMessage(Message<T> message) {
            this.message.received(message);
        }

        @Override
        public CompletableFuture<Message<T>> receiveAsync() {
            return message.receiveAsync();
        }
    }

    public static class Listener<T> extends WebSocketConsumer<T> {
        protected final MessageListener<T> listener;

        protected Listener(WebSocketPulsarClient client, WebSocketConsumerEndpoint endpoint, Schema<T> schema,
                           MessageListener<T> listener) {
            super(client, endpoint, schema);
            this.listener = listener;
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

        @Override
        public CompletableFuture<Message<T>> receiveAsync() {
            return CompletableFuture.failedFuture(new PulsarClientException.NotAllowedException("receive"));
        }
    }
}
