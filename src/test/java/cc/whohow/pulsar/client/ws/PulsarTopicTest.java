package cc.whohow.pulsar.client.ws;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class PulsarTopicTest {
    @Test
    public void test() {
        List<String> uris = Arrays.asList(
                "persistent://property/cluster/namespace/topic",
                "non-persistent://tenant/namespace/topic",
                "non-persistent://my-property/us-west/my-namespace/my-topic",
                "non-persistent://sample/standalone/ns1/my-topic",
                "persistent://my-property/global/my-namespace/my-topic"
        );

        for (String uri : uris) {
            PulsarTopic pulsarTopic = new PulsarTopic(uri);
            System.out.println(pulsarTopic);
            System.out.println(pulsarTopic.isPersistent());
            System.out.println(pulsarTopic.getProperty());
            System.out.println(pulsarTopic.getCluster());
            System.out.println(pulsarTopic.getNamespace());
            System.out.println(pulsarTopic.getTopic());
        }
    }
}
