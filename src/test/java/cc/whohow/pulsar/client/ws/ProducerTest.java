package cc.whohow.pulsar.client.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ProducerTest {
    @Test
    public void test() throws Exception {
        try (WebSocketPulsarClient pulsar = new WebSocketPulsarClient(HttpClient.newHttpClient(), new ObjectMapper(),
                "ws://127.0.0.1:8080/pulsar")) {
            try (Producer<JsonNode> producer = pulsar.newProducer(JsonNode.class)
                    .topic("persistent://t/n/topic")
                    .producerName("java-ws")
                    .create()) {
                List<CompletableFuture<MessageId>> ids = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    ids.add(producer.sendAsync(pulsar.newJsonObject().put("i", i)));
                }
                producer.flush();
                for (CompletableFuture<MessageId> id : ids) {
                    System.out.println(id.getNow(null));
                }
            }
        }
    }
}
