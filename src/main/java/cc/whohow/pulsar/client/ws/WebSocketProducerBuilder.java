package cc.whohow.pulsar.client.ws;

import org.apache.pulsar.client.api.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class WebSocketProducerBuilder<T> implements ProducerBuilder<T> {
    protected final WebSocketPulsarClient client;
    protected final Schema<T> schema;
    protected String topic;
    protected long sendTimeoutMillis;
    protected boolean batchingEnabled;
    protected int batchingMaxMessages;
    protected int maxPendingMessages;
    protected long batchingMaxPublishDelay;
    protected MessageRoutingMode messageRoutingMode;
    protected CompressionType compressionType;
    protected String producerName;
    protected long initialSequenceId;
    protected HashingScheme hashingScheme;
    protected String token;

    public WebSocketProducerBuilder(WebSocketPulsarClient client, Schema<T> schema) {
        this.client = client;
        this.schema = schema;
    }

    @Override
    public Producer<T> create() throws PulsarClientException {
        return PulsarWebSocket.await(createAsync());
    }

    @Override
    public CompletableFuture<Producer<T>> createAsync() {
        WebSocketProducerEndpoint endpoint = new WebSocketProducerEndpoint(client.getServiceUrl(), topic);
        endpoint.setTopic(topic);
        endpoint.setSendTimeoutMillis(sendTimeoutMillis);
        endpoint.setBatchingEnabled(batchingEnabled);
        endpoint.setBatchingMaxMessages(batchingMaxMessages);
        endpoint.setMaxPendingMessages(maxPendingMessages);
        endpoint.setBatchingMaxPublishDelay(batchingMaxPublishDelay);
        if (messageRoutingMode != null) {
            endpoint.setMessageRoutingMode(messageRoutingMode.name());
        }
        if (compressionType != null) {
            endpoint.setCompressionType(compressionType.name());
        }
        endpoint.setProducerName(producerName);
        endpoint.setInitialSequenceId(initialSequenceId);
        if (hashingScheme != null) {
            endpoint.setHashingScheme(hashingScheme.name());
        }
        endpoint.setToken(token);

        return new WebSocketProducer<>(client, endpoint, schema).connectAsync();
    }

    @Override
    public ProducerBuilder<T> loadConf(Map<String, Object> config) {
        return null;
    }

    @Override
    public ProducerBuilder<T> clone() {
        return null;
    }

    @Override
    public ProducerBuilder<T> topic(String topicName) {
        this.topic = topicName;
        return this;
    }

    @Override
    public ProducerBuilder<T> producerName(String producerName) {
        this.producerName = producerName;
        return this;
    }

    @Override
    public ProducerBuilder<T> sendTimeout(int sendTimeout, TimeUnit unit) {
        this.sendTimeoutMillis = unit.toMillis(sendTimeout);
        return this;
    }

    @Override
    public ProducerBuilder<T> maxPendingMessages(int maxPendingMessages) {
        this.maxPendingMessages = maxPendingMessages;
        return this;
    }

    @Override
    public ProducerBuilder<T> maxPendingMessagesAcrossPartitions(int maxPendingMessagesAcrossPartitions) {
        throw PulsarWebSocket.unsupportedUnchecked("Partition");
    }

    @Override
    public ProducerBuilder<T> blockIfQueueFull(boolean blockIfQueueFull) {
        return null;
    }

    @Override
    public ProducerBuilder<T> messageRoutingMode(MessageRoutingMode messageRoutingMode) {
        this.messageRoutingMode = messageRoutingMode;
        return this;
    }

    @Override
    public ProducerBuilder<T> hashingScheme(HashingScheme hashingScheme) {
        this.hashingScheme = hashingScheme;
        return this;
    }

    @Override
    public ProducerBuilder<T> compressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
        return this;
    }

    @Override
    public ProducerBuilder<T> messageRouter(MessageRouter messageRouter) {
        throw PulsarWebSocket.unsupportedUnchecked("MessageRouter");
    }

    @Override
    public ProducerBuilder<T> enableBatching(boolean enableBatching) {
        this.batchingEnabled = enableBatching;
        return this;
    }

    @Override
    public ProducerBuilder<T> enableChunking(boolean enableChunking) {
        return this;
    }

    @Override
    public ProducerBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        throw PulsarWebSocket.unsupportedUnchecked("Crypto");
    }

    @Override
    public ProducerBuilder<T> defaultCryptoKeyReader(String publicKey) {
        throw PulsarWebSocket.unsupportedUnchecked("Crypto");
    }

    @Override
    public ProducerBuilder<T> defaultCryptoKeyReader(Map<String, String> publicKeys) {
        throw PulsarWebSocket.unsupportedUnchecked("Crypto");
    }

    @Override
    public ProducerBuilder<T> addEncryptionKey(String key) {
        throw PulsarWebSocket.unsupportedUnchecked("Crypto");
    }

    @Override
    public ProducerBuilder<T> cryptoFailureAction(ProducerCryptoFailureAction action) {
        throw PulsarWebSocket.unsupportedUnchecked("Crypto");
    }

    @Override
    public ProducerBuilder<T> batchingMaxPublishDelay(long batchDelay, TimeUnit timeUnit) {
        this.batchingMaxPublishDelay = timeUnit.toMillis(batchDelay);
        return this;
    }

    @Override
    public ProducerBuilder<T> roundRobinRouterBatchingPartitionSwitchFrequency(int frequency) {
        throw PulsarWebSocket.unsupportedUnchecked("Partition");
    }

    @Override
    public ProducerBuilder<T> batchingMaxMessages(int batchMessagesMaxMessagesPerBatch) {
        this.batchingMaxMessages = batchMessagesMaxMessagesPerBatch;
        return this;
    }

    @Override
    public ProducerBuilder<T> batchingMaxBytes(int batchingMaxBytes) {
        throw PulsarWebSocket.unsupportedUnchecked("BatchingMaxBytes");
    }

    @Override
    public ProducerBuilder<T> batcherBuilder(BatcherBuilder batcherBuilder) {
        return null;
    }

    @Override
    public ProducerBuilder<T> initialSequenceId(long initialSequenceId) {
        this.initialSequenceId = initialSequenceId;
        return this;
    }

    @Override
    public ProducerBuilder<T> property(String key, String value) {
        return null;
    }

    @Override
    public ProducerBuilder<T> properties(Map<String, String> properties) {
        return null;
    }

    @Override
    public ProducerBuilder<T> intercept(ProducerInterceptor<T>... interceptors) {
        return null;
    }

    @Override
    public ProducerBuilder<T> intercept(org.apache.pulsar.client.api.interceptor.ProducerInterceptor... interceptors) {
        return null;
    }

    @Override
    public ProducerBuilder<T> autoUpdatePartitions(boolean autoUpdate) {
        return null;
    }

    @Override
    public ProducerBuilder<T> autoUpdatePartitionsInterval(int interval, TimeUnit unit) {
        return null;
    }

    @Override
    public ProducerBuilder<T> enableMultiSchema(boolean multiSchema) {
        return null;
    }
}
