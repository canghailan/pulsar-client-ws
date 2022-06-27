package cc.whohow.pulsar.client.ws;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class WebSocketConsumerEndpoint extends WebSocketEndpoint {
    private String subscription;
    private long ackTimeoutMillis;
    private String subscriptionType;
    private int receiverQueueSize;
    private String consumerName;
    private int priorityLevel;
    private int maxRedeliverCount;
    private String deadLetterTopic;
    private boolean pullMode;
    private int negativeAckRedeliveryDelay;
    private String token;

    public WebSocketConsumerEndpoint(String serviceUrl, String topic, String subscription) {
        super(serviceUrl, topic);
        Objects.requireNonNull(subscription);
        this.subscription = subscription;
    }

    public String getSubscription() {
        return subscription;
    }

    public void setSubscription(String subscription) {
        this.subscription = subscription;
    }

    public long getAckTimeoutMillis() {
        return ackTimeoutMillis;
    }

    public void setAckTimeoutMillis(long ackTimeoutMillis) {
        this.ackTimeoutMillis = ackTimeoutMillis;
    }

    public String getSubscriptionType() {
        return subscriptionType;
    }

    public void setSubscriptionType(String subscriptionType) {
        this.subscriptionType = subscriptionType;
    }

    public int getReceiverQueueSize() {
        return receiverQueueSize;
    }

    public void setReceiverQueueSize(int receiverQueueSize) {
        this.receiverQueueSize = receiverQueueSize;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    public int getPriorityLevel() {
        return priorityLevel;
    }

    public void setPriorityLevel(int priorityLevel) {
        this.priorityLevel = priorityLevel;
    }

    public int getMaxRedeliverCount() {
        return maxRedeliverCount;
    }

    public void setMaxRedeliverCount(int maxRedeliverCount) {
        this.maxRedeliverCount = maxRedeliverCount;
    }

    public String getDeadLetterTopic() {
        return deadLetterTopic;
    }

    public void setDeadLetterTopic(String deadLetterTopic) {
        this.deadLetterTopic = deadLetterTopic;
    }

    public boolean isPullMode() {
        return pullMode;
    }

    public void setPullMode(boolean pullMode) {
        this.pullMode = pullMode;
    }

    public int getNegativeAckRedeliveryDelay() {
        return negativeAckRedeliveryDelay;
    }

    public void setNegativeAckRedeliveryDelay(int negativeAckRedeliveryDelay) {
        this.negativeAckRedeliveryDelay = negativeAckRedeliveryDelay;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    @Override
    public String toString() {
        PulsarTopic pulsarTopic = new PulsarTopic(topic);
        StringBuilder uri = new StringBuilder();
        uri.append(serviceUrl);
        if (!serviceUrl.endsWith("/")) {
            uri.append("/");
        }
        uri.append("ws/v2/consumer/");
        uri.append(pulsarTopic.isPersistent() ? "persistent" : "non-persistent");
        uri.append("/");
        uri.append(pulsarTopic.getProperty());
        uri.append("/");
        uri.append(pulsarTopic.getNamespace());
        uri.append("/");
        uri.append(pulsarTopic.getTopic());
        uri.append("/");
        uri.append(subscription);
        String sep = "?";
        if (ackTimeoutMillis > 0) {
            uri.append(sep).append("ackTimeoutMillis").append("=").append(ackTimeoutMillis);
            sep = "&";
        }
        if (subscriptionType != null) {
            uri.append(sep).append("subscriptionType").append("=").append(subscriptionType);
            sep = "&";
        }
        if (receiverQueueSize > 0) {
            uri.append(sep).append("receiverQueueSize").append("=").append(receiverQueueSize);
            sep = "&";
        }
        if (consumerName != null) {
            uri.append(sep).append("consumerName").append("=").append(URLEncoder.encode(consumerName, StandardCharsets.UTF_8));
            sep = "&";
        }
        if (priorityLevel > 0) {
            uri.append(sep).append("priorityLevel").append("=").append(priorityLevel);
            sep = "&";
        }
        if (maxRedeliverCount > 0) {
            uri.append(sep).append("maxRedeliverCount").append("=").append(maxRedeliverCount);
            sep = "&";
        }
        if (deadLetterTopic != null) {
            uri.append(sep).append("deadLetterTopic").append("=").append(URLEncoder.encode(deadLetterTopic, StandardCharsets.UTF_8));
            sep = "&";
        }
        if (pullMode) {
            uri.append(sep).append("pullMode=true");
            sep = "&";
        }
        if (negativeAckRedeliveryDelay > 0) {
            uri.append(sep).append("negativeAckRedeliveryDelay").append("=").append(negativeAckRedeliveryDelay);
            sep = "&";
        }
        if (token != null) {
            uri.append(sep).append("token").append("=").append(token);
        }
        return uri.toString();
    }
}
