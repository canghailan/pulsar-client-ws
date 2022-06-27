package cc.whohow.pulsar.client.ws;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class WebSocketProducerEndpoint extends WebSocketEndpoint {
    private long sendTimeoutMillis;
    private boolean batchingEnabled;
    private int batchingMaxMessages;
    private int maxPendingMessages;
    private long batchingMaxPublishDelay;
    private String messageRoutingMode;
    private String compressionType;
    private String producerName;
    private long initialSequenceId;
    private String hashingScheme;
    private String token;

    public WebSocketProducerEndpoint(String serviceUrl, String topic) {
        super(serviceUrl, topic);
    }

    public long getSendTimeoutMillis() {
        return sendTimeoutMillis;
    }

    public void setSendTimeoutMillis(long sendTimeoutMillis) {
        this.sendTimeoutMillis = sendTimeoutMillis;
    }

    public boolean isBatchingEnabled() {
        return batchingEnabled;
    }

    public void setBatchingEnabled(boolean batchingEnabled) {
        this.batchingEnabled = batchingEnabled;
    }

    public int getBatchingMaxMessages() {
        return batchingMaxMessages;
    }

    public void setBatchingMaxMessages(int batchingMaxMessages) {
        this.batchingMaxMessages = batchingMaxMessages;
    }

    public int getMaxPendingMessages() {
        return maxPendingMessages;
    }

    public void setMaxPendingMessages(int maxPendingMessages) {
        this.maxPendingMessages = maxPendingMessages;
    }

    public long getBatchingMaxPublishDelay() {
        return batchingMaxPublishDelay;
    }

    public void setBatchingMaxPublishDelay(long batchingMaxPublishDelay) {
        this.batchingMaxPublishDelay = batchingMaxPublishDelay;
    }

    public String getMessageRoutingMode() {
        return messageRoutingMode;
    }

    public void setMessageRoutingMode(String messageRoutingMode) {
        this.messageRoutingMode = messageRoutingMode;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(String compressionType) {
        this.compressionType = compressionType;
    }

    public String getProducerName() {
        return producerName;
    }

    public void setProducerName(String producerName) {
        this.producerName = producerName;
    }

    public long getInitialSequenceId() {
        return initialSequenceId;
    }

    public void setInitialSequenceId(long initialSequenceId) {
        this.initialSequenceId = initialSequenceId;
    }

    public String getHashingScheme() {
        return hashingScheme;
    }

    public void setHashingScheme(String hashingScheme) {
        this.hashingScheme = hashingScheme;
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
        uri.append("ws/v2/producer/");
        uri.append(pulsarTopic.isPersistent() ? "persistent" : "non-persistent");
        uri.append("/");
        uri.append(pulsarTopic.getProperty());
        uri.append("/");
        uri.append(pulsarTopic.getNamespace());
        uri.append("/");
        uri.append(pulsarTopic.getTopic());
        String sep = "?";
        if (sendTimeoutMillis > 0) {
            uri.append(sep).append("sendTimeoutMillis").append("=").append(sendTimeoutMillis);
            sep = "&";
        }
        if (batchingEnabled) {
            uri.append(sep).append("batchingEnabled=true");
            sep = "&";
        }
        if (batchingMaxMessages > 0) {
            uri.append(sep).append("batchingMaxMessages").append("=").append(batchingMaxMessages);
            sep = "&";
        }
        if (maxPendingMessages > 0) {
            uri.append(sep).append("maxPendingMessages").append("=").append(maxPendingMessages);
            sep = "&";
        }
        if (batchingMaxPublishDelay > 0) {
            uri.append(sep).append("batchingMaxPublishDelay").append("=").append(batchingMaxPublishDelay);
            sep = "&";
        }
        if (messageRoutingMode != null) {
            uri.append(sep).append("messageRoutingMode").append("=").append(messageRoutingMode);
            sep = "&";
        }
        if (compressionType != null) {
            uri.append(sep).append("compressionType").append("=").append(compressionType);
            sep = "&";
        }
        if (producerName != null) {
            uri.append(sep).append("producerName").append("=").append(URLEncoder.encode(producerName, StandardCharsets.UTF_8));
            sep = "&";
        }
        if (initialSequenceId > 0) {
            uri.append(sep).append("initialSequenceId").append("=").append(initialSequenceId);
            sep = "&";
        }
        if (hashingScheme != null) {
            uri.append(sep).append("hashingScheme").append("=").append(hashingScheme);
            sep = "&";
        }
        if (token != null) {
            uri.append(sep).append("token").append("=").append(token);
        }
        return uri.toString();
    }
}
