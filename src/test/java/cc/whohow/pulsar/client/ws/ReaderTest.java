package cc.whohow.pulsar.client.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.api.Reader;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;

public class ReaderTest {
    @Test
    public void test() throws Exception {
        try (WebSocketPulsarClient pulsar = new WebSocketPulsarClient(HttpClient.newHttpClient(), new ObjectMapper(),
                "ws://127.0.0.1:8080/pulsar")) {
            try (Reader<JsonNode> consumer = pulsar.newReader(JsonNode.class)
                    .topic("persistent://t/n/topic")
                    .create()) {
                while (true){
                    System.out.println(consumer.readNext().getValue());
                }
            }
        }
    }
}
