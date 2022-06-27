package cc.whohow.pulsar.client.ws;

import org.apache.pulsar.client.api.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class WebSocketReaderBuilder<T> implements ReaderBuilder<T> {
    protected final WebSocketPulsarClient client;
    protected final Schema<T> schema;
    protected String topic;
    protected String readerName;
    protected int receiverQueueSize;
    protected String messageId;
    protected String token;
    protected ReaderListener<T> readerListener;

    public WebSocketReaderBuilder(WebSocketPulsarClient client, Schema<T> schema) {
        this.client = client;
        this.schema = schema;
    }

    @Override
    public Reader<T> create() throws PulsarClientException {
        return PulsarWebSocket.await(createAsync());
    }

    @Override
    public CompletableFuture<Reader<T>> createAsync() {
        WebSocketReaderEndpoint endpoint = new WebSocketReaderEndpoint(client.getServiceUrl(), topic);
        endpoint.setReaderName(readerName);
        endpoint.setReceiverQueueSize(receiverQueueSize);
        endpoint.setMessageId(messageId);
        endpoint.setToken(token);

        if (readerListener == null) {
            return new WebSocketReader.Queue<>(client, endpoint, schema).connectAsync();
        } else {
            return new WebSocketReader.Listener<>(client, endpoint, schema, readerListener).connectAsync();
        }
    }

    @Override
    public ReaderBuilder<T> loadConf(Map<String, Object> config) {
        return null;
    }

    @Override
    public ReaderBuilder<T> clone() {
        return null;
    }

    @Override
    public ReaderBuilder<T> topic(String topicName) {
        this.topic = topicName;
        return this;
    }

    @Override
    public ReaderBuilder<T> startMessageId(MessageId startMessageId) {
        this.messageId = PulsarWebSocket.toJsonMessageId(startMessageId);
        return this;
    }

    @Override
    public ReaderBuilder<T> startMessageFromRollbackDuration(long rollbackDuration, TimeUnit timeunit) {
        throw PulsarWebSocket.unsupportedUnchecked("startMessageFromRollbackDuration");
    }

    @Override
    public ReaderBuilder<T> startMessageIdInclusive() {
        throw PulsarWebSocket.unsupportedUnchecked("startMessageIdInclusive");
    }

    @Override
    public ReaderBuilder<T> readerListener(ReaderListener<T> readerListener) {
        this.readerListener = readerListener;
        return this;
    }

    @Override
    public ReaderBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        throw PulsarWebSocket.unsupportedUnchecked("CryptoKeyReader");
    }

    @Override
    public ReaderBuilder<T> defaultCryptoKeyReader(String privateKey) {
        throw PulsarWebSocket.unsupportedUnchecked("CryptoKeyReader");
    }

    @Override
    public ReaderBuilder<T> defaultCryptoKeyReader(Map<String, String> privateKeys) {
        throw PulsarWebSocket.unsupportedUnchecked("CryptoKeyReader");
    }

    @Override
    public ReaderBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action) {
        throw PulsarWebSocket.unsupportedUnchecked("CryptoKeyReader");
    }

    @Override
    public ReaderBuilder<T> receiverQueueSize(int receiverQueueSize) {
        this.receiverQueueSize = receiverQueueSize;
        return this;
    }

    @Override
    public ReaderBuilder<T> readerName(String readerName) {
        this.readerName = readerName;
        return this;
    }

    @Override
    public ReaderBuilder<T> subscriptionRolePrefix(String subscriptionRolePrefix) {
        throw PulsarWebSocket.unsupportedUnchecked("subscriptionRolePrefix");
    }

    @Override
    public ReaderBuilder<T> readCompacted(boolean readCompacted) {
        throw PulsarWebSocket.unsupportedUnchecked("readCompacted");
    }

    @Override
    public ReaderBuilder<T> keyHashRange(Range... ranges) {
        throw PulsarWebSocket.unsupportedUnchecked("keyHashRange");
    }
}
