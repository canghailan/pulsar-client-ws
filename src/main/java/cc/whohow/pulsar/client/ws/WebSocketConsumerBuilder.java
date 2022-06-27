package cc.whohow.pulsar.client.ws;

import org.apache.pulsar.client.api.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class WebSocketConsumerBuilder<T> implements ConsumerBuilder<T> {
    protected final WebSocketPulsarClient client;
    protected final Schema<T> schema;
    protected String topic;
    protected String subscription;
    protected long ackTimeoutMillis;
    protected SubscriptionType subscriptionType;
    protected int receiverQueueSize;
    protected String consumerName;
    protected int priorityLevel;
    protected int maxRedeliverCount;
    protected String deadLetterTopic;
    protected boolean pullMode;
    protected int negativeAckRedeliveryDelay;
    protected String token;
    protected MessageListener<T> messageListener;

    public WebSocketConsumerBuilder(WebSocketPulsarClient client, Schema<T> schema) {
        this.client = client;
        this.schema = schema;
    }

    @Override
    public ConsumerBuilder<T> clone() {
        return null;
    }

    @Override
    public ConsumerBuilder<T> loadConf(Map<String, Object> config) {
        return null;
    }

    @Override
    public Consumer<T> subscribe() throws PulsarClientException {
        return PulsarWebSocket.await(subscribeAsync());
    }

    @Override
    public CompletableFuture<Consumer<T>> subscribeAsync() {
        WebSocketConsumerEndpoint endpoint = new WebSocketConsumerEndpoint(client.getServiceUrl(), topic, subscription);
        endpoint.setAckTimeoutMillis(ackTimeoutMillis);
        if (subscriptionType != null) {
            endpoint.setSubscriptionType(subscriptionType.name());
        }
        endpoint.setReceiverQueueSize(receiverQueueSize);
        endpoint.setConsumerName(consumerName);
        endpoint.setPriorityLevel(priorityLevel);
        endpoint.setMaxRedeliverCount(maxRedeliverCount);
        endpoint.setDeadLetterTopic(deadLetterTopic);
        endpoint.setPullMode(pullMode);
        endpoint.setNegativeAckRedeliveryDelay(negativeAckRedeliveryDelay);
        endpoint.setToken(token);

        if (messageListener == null) {
            return new WebSocketConsumer.Queue<>(client, endpoint, schema).connectAsync();
        } else {
            return new WebSocketConsumer.Listener<>(client, endpoint, schema, messageListener).connectAsync();
        }
    }

    @Override
    public ConsumerBuilder<T> topic(String... topicNames) {
        if (topicNames.length == 1) {
            this.topic = topicNames[0];
            return this;
        }
        throw new IllegalArgumentException(String.join(",", topicNames));
    }

    @Override
    public ConsumerBuilder<T> topics(List<String> topicNames) {
        if (topicNames.size() == 1) {
            this.topic = topicNames.get(0);
            return this;
        }
        throw new IllegalArgumentException(String.join(",", topicNames));
    }

    @Override
    public ConsumerBuilder<T> topicsPattern(Pattern topicsPattern) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> topicsPattern(String topicsPattern) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> subscriptionName(String subscriptionName) {
        this.subscription = subscriptionName;
        return this;
    }

    @Override
    public ConsumerBuilder<T> ackTimeout(long ackTimeout, TimeUnit timeUnit) {
        this.ackTimeoutMillis = timeUnit.toMillis(ackTimeout);
        return this;
    }

    @Override
    public ConsumerBuilder<T> ackTimeoutTickTime(long tickTime, TimeUnit timeUnit) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> negativeAckRedeliveryDelay(long redeliveryDelay, TimeUnit timeUnit) {
        this.negativeAckRedeliveryDelay = (int) timeUnit.toMillis(redeliveryDelay);
        return null;
    }

    @Override
    public ConsumerBuilder<T> subscriptionType(SubscriptionType subscriptionType) {
        this.subscriptionType = subscriptionType;
        return this;
    }

    @Override
    public ConsumerBuilder<T> subscriptionMode(SubscriptionMode subscriptionMode) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> messageListener(MessageListener<T> messageListener) {
        this.messageListener = messageListener;
        return this;
    }

    @Override
    public ConsumerBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> defaultCryptoKeyReader(String privateKey) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> defaultCryptoKeyReader(Map<String, String> privateKeys) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> messageCrypto(MessageCrypto messageCrypto) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> receiverQueueSize(int receiverQueueSize) {
        this.receiverQueueSize = receiverQueueSize;
        return this;
    }

    @Override
    public ConsumerBuilder<T> acknowledgmentGroupTime(long delay, TimeUnit unit) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> replicateSubscriptionState(boolean replicateSubscriptionState) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> maxTotalReceiverQueueSizeAcrossPartitions(int maxTotalReceiverQueueSizeAcrossPartitions) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> consumerName(String consumerName) {
        this.consumerName = consumerName;
        return this;
    }

    @Override
    public ConsumerBuilder<T> consumerEventListener(ConsumerEventListener consumerEventListener) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> readCompacted(boolean readCompacted) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> patternAutoDiscoveryPeriod(int periodInMinutes) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> patternAutoDiscoveryPeriod(int interval, TimeUnit unit) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> priorityLevel(int priorityLevel) {
        this.priorityLevel = priorityLevel;
        return this;
    }

    @Override
    public ConsumerBuilder<T> property(String key, String value) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> properties(Map<String, String> properties) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> subscriptionInitialPosition(SubscriptionInitialPosition subscriptionInitialPosition) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> subscriptionTopicsMode(RegexSubscriptionMode regexSubscriptionMode) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> intercept(ConsumerInterceptor<T>... interceptors) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> deadLetterPolicy(DeadLetterPolicy deadLetterPolicy) {
        this.maxRedeliverCount = deadLetterPolicy.getMaxRedeliverCount();
        this.deadLetterTopic = deadLetterPolicy.getDeadLetterTopic();
        return null;
    }

    @Override
    public ConsumerBuilder<T> autoUpdatePartitions(boolean autoUpdate) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> autoUpdatePartitionsInterval(int interval, TimeUnit unit) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> keySharedPolicy(KeySharedPolicy keySharedPolicy) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> startMessageIdInclusive() {
        return null;
    }

    @Override
    public ConsumerBuilder<T> batchReceivePolicy(BatchReceivePolicy batchReceivePolicy) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> enableRetry(boolean retryEnable) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> enableBatchIndexAcknowledgment(boolean batchIndexAcknowledgmentEnabled) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> maxPendingChuckedMessage(int maxPendingChuckedMessage) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> autoAckOldestChunkedMessageOnQueueFull(boolean autoAckOldestChunkedMessageOnQueueFull) {
        return null;
    }

    @Override
    public ConsumerBuilder<T> expireTimeOfIncompleteChunkedMessage(long duration, TimeUnit unit) {
        return null;
    }
}
