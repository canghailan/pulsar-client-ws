package cc.whohow.pulsar.client.ws;

import java.util.Objects;

public class WebSocketEndpoint {
    protected String serviceUrl;
    protected String topic;

    public WebSocketEndpoint(String serviceUrl, String topic) {
        Objects.requireNonNull(serviceUrl);
        Objects.requireNonNull(topic);
        this.serviceUrl = serviceUrl;
        this.topic = topic;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public void setServiceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
