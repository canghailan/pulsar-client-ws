package cc.whohow.pulsar.client.ws;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class WebSocketReaderEndpoint extends WebSocketEndpoint {
    private String readerName;
    private int receiverQueueSize;
    private String messageId;
    private String token;

    public WebSocketReaderEndpoint(String serviceUrl, String topic) {
        super(serviceUrl, topic);
    }

    public String getReaderName() {
        return readerName;
    }

    public void setReaderName(String readerName) {
        this.readerName = readerName;
    }

    public int getReceiverQueueSize() {
        return receiverQueueSize;
    }

    public void setReceiverQueueSize(int receiverQueueSize) {
        this.receiverQueueSize = receiverQueueSize;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
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
        uri.append("ws/v2/reader/");
        uri.append(pulsarTopic.isPersistent() ? "persistent" : "non-persistent");
        uri.append("/");
        uri.append(pulsarTopic.getProperty());
        uri.append("/");
        uri.append(pulsarTopic.getNamespace());
        uri.append("/");
        uri.append(pulsarTopic.getTopic());
        String sep = "?";
        if (readerName != null) {
            uri.append(sep).append("readerName").append("=").append(URLEncoder.encode(readerName, StandardCharsets.UTF_8));
            sep = "&";
        }
        if (receiverQueueSize > 0) {
            uri.append(sep).append("receiverQueueSize").append("=").append(receiverQueueSize);
            sep = "&";
        }
        if (messageId != null) {
            uri.append(sep).append("messageId").append("=").append(messageId);
            sep = "&";
        }
        if (token != null) {
            uri.append(sep).append("token").append("=").append(token);
        }
        return uri.toString();
    }
}
