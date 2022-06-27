package cc.whohow.pulsar.client.ws;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class WebSocketTypedMessageBuilder<T> implements TypedMessageBuilder<T> {
    protected final WebSocketProducer<?> producer;
    protected final Schema<T> schema;
    protected final ObjectNode message;

    public WebSocketTypedMessageBuilder(WebSocketProducer<?> producer, Schema<T> schema, ObjectNode message) {
        this.producer = producer;
        this.schema = schema;
        this.message = message;
    }

    protected ObjectNode properties() {
        ObjectNode properties = (ObjectNode) message.get(JsonMessage.PROPERTIES);
        if (properties == null) {
            properties = message.putObject(JsonMessage.PROPERTIES);
        }
        return properties;
    }

    @Override
    public MessageId send() throws PulsarClientException {
        return producer.send(message);
    }

    @Override
    public CompletableFuture<MessageId> sendAsync() {
        return producer.sendAsync(message);
    }

    @Override
    public TypedMessageBuilder<T> key(String key) {
        message.put(JsonMessage.KEY, key);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> keyBytes(byte[] key) {
        message.put(JsonMessage.KEY, key);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> orderingKey(byte[] orderingKey) {
        // ignore
        return this;
    }

    @Override
    public TypedMessageBuilder<T> value(T value) {
        message.put(JsonMessage.PAYLOAD, schema.encode(value));
        return this;
    }

    @Override
    public TypedMessageBuilder<T> property(String name, String value) {
        properties().put(name, value);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> properties(Map<String, String> properties) {
        ObjectNode p = properties();
        for (Map.Entry<String, String> e : properties.entrySet()) {
            p.put(e.getKey(), e.getValue());
        }
        return this;
    }

    @Override
    public TypedMessageBuilder<T> eventTime(long timestamp) {
        // ignore
        return this;
    }

    @Override
    public TypedMessageBuilder<T> sequenceId(long sequenceId) {
        // ignore
        return this;
    }

    @Override
    public TypedMessageBuilder<T> replicationClusters(List<String> clusters) {
        ArrayNode replicationClusters = message.putArray(JsonMessage.REPLICATION_CLUSTERS);
        for (String cluster : clusters) {
            replicationClusters.add(cluster);
        }
        return this;
    }

    @Override
    public TypedMessageBuilder<T> disableReplication() {
        message.remove(JsonMessage.REPLICATION_CLUSTERS);
        return this;
    }

    @Override
    public TypedMessageBuilder<T> deliverAt(long timestamp) {
        throw PulsarWebSocket.unsupportedUnchecked("deliverAt");
    }

    @Override
    public TypedMessageBuilder<T> deliverAfter(long delay, TimeUnit unit) {
        throw PulsarWebSocket.unsupportedUnchecked("deliverAfter");
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypedMessageBuilder<T> loadConf(Map<String, Object> config) {
        for (Map.Entry<String, Object> e : config.entrySet()) {
            switch (e.getKey()) {
                case CONF_KEY: {
                    key((String) e.getValue());
                    break;
                }
                case CONF_PROPERTIES: {
                    properties((Map<String, String>) e.getValue());
                    break;
                }
                case CONF_EVENT_TIME: {
                    eventTime((Long) e.getValue());
                    break;
                }
                case CONF_SEQUENCE_ID: {
                    sequenceId((Long) e.getValue());
                    break;
                }
                case CONF_REPLICATION_CLUSTERS: {
                    replicationClusters((List<String>) e.getValue());
                    break;
                }
                case CONF_DISABLE_REPLICATION: {
                    if ((Boolean) e.getValue()) {
                        disableReplication();
                    }
                    break;
                }
                case CONF_DELIVERY_AFTER_SECONDS: {
                    deliverAfter((Long) e.getValue(), TimeUnit.SECONDS);
                    break;
                }
                case CONF_DELIVERY_AT: {
                    deliverAt((Long) e.getValue());
                }
            }
        }
        return this;
    }

    public TypedMessageBuilder<T> context(String context) {
        message.put(JsonMessage.CONTEXT, context);
        return this;
    }
}
