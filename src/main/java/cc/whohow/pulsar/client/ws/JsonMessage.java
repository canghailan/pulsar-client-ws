package cc.whohow.pulsar.client.ws;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.EncryptionContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;

public class JsonMessage<T> implements Message<T> {
    public static final String MESSAGE_ID = "messageId";
    public static final String PAYLOAD = "payload";
    public static final String PUBLISH_TIME = "publishTime";
    public static final String REDELIVERY_COUNT = "redeliveryCount";
    public static final String PROPERTIES = "properties";
    public static final String KEY = "key";
    public static final String ENCRYPTION_CONTEXT = "encryptionContext";
    public static final String REPLICATION_CLUSTERS = "replicationClusters";
    public static final String CONTEXT = "context";

    protected final String topic;
    protected final Schema<T> schema;
    protected final JsonNode message;
    protected volatile T value;

    public JsonMessage(String topic, Schema<T> schema, JsonNode message) {
        this.topic = topic;
        this.schema = schema;
        this.message = message;
    }

    public static <V> Map<String, V> toMap(JsonNode object, Function<JsonNode, V> value) {
        if (object.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, V> map = new LinkedHashMap<>(object.size());
        Iterator<Map.Entry<String, JsonNode>> fields = object.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> e = fields.next();
            map.put(e.getKey(), value.apply(e.getValue()));
        }
        return map;
    }

    @Override
    public Map<String, String> getProperties() {
        return toMap(message.path(PROPERTIES), JsonNode::textValue);
    }

    @Override
    public boolean hasProperty(String name) {
        return message.path(PROPERTIES).has(name);
    }

    @Override
    public String getProperty(String name) {
        return message.path(PROPERTIES).path(name).textValue();
    }

    @Override
    public byte[] getData() {
        try {
            return message.path(PAYLOAD).binaryValue();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public T getValue() {
        if (value == null) {
            value = schema.decode(getData());
        }
        return value;
    }

    @Override
    public MessageId getMessageId() {
        return new Id(message.path(MESSAGE_ID).textValue());
    }

    @Override
    public long getPublishTime() {
        return message.path(PUBLISH_TIME).longValue();
    }

    @Override
    public long getEventTime() {
        return 0;
    }

    @Override
    public long getSequenceId() {
        return 0;
    }

    @Override
    public String getProducerName() {
        return null;
    }

    @Override
    public boolean hasKey() {
        return message.has(KEY);
    }

    @Override
    public String getKey() {
        return message.path(KEY).textValue();
    }

    @Override
    public boolean hasBase64EncodedKey() {
        return false;
    }

    @Override
    public byte[] getKeyBytes() {
        return getKey().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean hasOrderingKey() {
        return false;
    }

    @Override
    public byte[] getOrderingKey() {
        return null;
    }

    @Override
    public String getTopicName() {
        return topic;
    }

    @Override
    public Optional<EncryptionContext> getEncryptionCtx() {
        JsonNode encryptionContext = message.path(ENCRYPTION_CONTEXT);
        if (encryptionContext.isNull() || encryptionContext.isMissingNode()) {
            return Optional.empty();
        }

        try {
            EncryptionContext encryptionCtx = new EncryptionContext();
            encryptionCtx.setKeys(toMap(encryptionContext.path("keys"), (k) -> {
                try {
                    EncryptionContext.EncryptionKey encryptionKey = new EncryptionContext.EncryptionKey();
                    encryptionKey.setKeyValue(k.path("keyValue").binaryValue());
                    encryptionKey.setMetadata(toMap(k.path("metadata"), JsonNode::textValue));
                    return encryptionKey;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }));
            encryptionCtx.setParam(encryptionContext.path("param").binaryValue());
            encryptionCtx.setAlgorithm(encryptionContext.path("algorithm").textValue());
            encryptionCtx.setCompressionType(Optional.ofNullable(encryptionContext.get("compressionType"))
                    .map(JsonNode::textValue)
                    .map(CompressionType::valueOf)
                    .orElse(CompressionType.NONE));
            encryptionCtx.setUncompressedMessageSize(encryptionContext.path("uncompressedMessageSize").asInt());
            encryptionCtx.setBatchSize(Optional.ofNullable(encryptionContext.get("batchSize"))
                    .map(JsonNode::asInt));
            return Optional.of(encryptionCtx);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getRedeliveryCount() {
        return message.path(REDELIVERY_COUNT).intValue();
    }

    @Override
    public byte[] getSchemaVersion() {
        return null;
    }

    @Override
    public boolean isReplicated() {
        return false;
    }

    @Override
    public String getReplicatedFrom() {
        return null;
    }

    public String getContext() {
        return message.path(CONTEXT).textValue();
    }

    public static class Id implements MessageId {
        public static Id EARLIEST = new Id("earliest");
        public static Id LATEST = new Id("latest");
        protected final String messageId;

        public Id(String messageId) {
            this.messageId = messageId;
        }

        @Override
        public byte[] toByteArray() {
            return messageId.getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public int compareTo(MessageId o) {
            return toString().compareTo(o.toString());
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof MessageId && toString().equals(o.toString());
        }

        @Override
        public int hashCode() {
            return Objects.hash(messageId);
        }

        @Override
        public String toString() {
            return messageId;
        }
    }
}
