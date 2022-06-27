package cc.whohow.pulsar.client.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Reader;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;

public class ConsumerTest {
    @Test
    public void test() throws Exception {
        try (WebSocketPulsarClient pulsar = new WebSocketPulsarClient(HttpClient.newHttpClient(), new ObjectMapper(),
                "ws://127.0.0.1:8080/pulsar")) {
            try (Consumer<JsonNode> consumer = pulsar.newConsumer(JsonNode.class)
                    .topic("persistent://t/n/topic")
                    .subscriptionName("sss")
                    .receiverQueueSize(1)
                    .subscribe()) {
                while (true){
                    Message<JsonNode> message = consumer.receive();
                    System.out.println(message.getValue());
                    consumer.acknowledgeAsync(message.getMessageId());
                }
            }
        }
    }
}
