package cc.whohow.pulsar.client.ws;

import org.apache.pulsar.client.api.PulsarClientException;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PulsarTopic {
    public static final Pattern TOPIC = Pattern.compile(
            "(?<persistent>persistent|non-persistent)://(?<property>[^/]+)(/(?<cluster>[^/]+))?/(?<namespace>[^/]+)/(?<topic>[^/]+)");
    private final boolean persistent;
    private final String property;
    private final String cluster;
    private final String namespace;
    private final String topic;

    public PulsarTopic(String uri) {
        Matcher matcher = TOPIC.matcher(uri);
        if (matcher.matches()) {
            persistent = "persistent".equals(matcher.group("persistent"));
            property = matcher.group("property");
            cluster = matcher.group("cluster");
            namespace = matcher.group("namespace");
            topic = matcher.group("topic");
        } else {
            throw new IllegalArgumentException(new PulsarClientException.InvalidTopicNameException(uri));
        }
    }

    public PulsarTopic(boolean persistent, String property, String cluster, String namespace, String topic) {
        this.persistent = persistent;
        this.property = property;
        this.cluster = cluster;
        this.namespace = namespace;
        this.topic = topic;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public String getProperty() {
        return property;
    }

    public String getCluster() {
        return cluster;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PulsarTopic)) return false;
        PulsarTopic that = (PulsarTopic) o;
        return persistent == that.persistent && property.equals(that.property) && Objects.equals(cluster, that.cluster) && namespace.equals(that.namespace) && topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(persistent, property, cluster, namespace, topic);
    }

    @Override
    public String toString() {
        if (cluster == null) {
            return (persistent ? "persistent" : "non-persistent") + "://" +
                    property + "/" +
                    namespace + "/" +
                    topic;
        } else {
            return (persistent ? "persistent" : "non-persistent") + "://" +
                    property + "/" +
                    cluster + "/" +
                    namespace + "/" +
                    topic;
        }
    }
}
