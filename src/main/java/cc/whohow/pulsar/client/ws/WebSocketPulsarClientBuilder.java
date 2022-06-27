package cc.whohow.pulsar.client.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.api.*;

import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class WebSocketPulsarClientBuilder implements ClientBuilder {
    protected final HttpClient.Builder httpClientBuilder = HttpClient.newBuilder();
    protected final ObjectMapper json = new ObjectMapper();
    protected String serviceUrl;

    @Override
    public WebSocketPulsarClient build() throws PulsarClientException {
        return new WebSocketPulsarClient(httpClientBuilder.build(), json, serviceUrl);
    }

    @Override
    public ClientBuilder loadConf(Map<String, Object> config) {
        return null;
    }

    @Override
    public ClientBuilder clone() {
        return null;
    }

    @Override
    public ClientBuilder serviceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
        return this;
    }

    @Override
    public ClientBuilder serviceUrlProvider(ServiceUrlProvider serviceUrlProvider) {
        return null;
    }

    @Override
    public ClientBuilder listenerName(String name) {
        return null;
    }

    @Override
    public ClientBuilder authentication(Authentication authentication) {
        return null;
    }

    @Override
    public ClientBuilder authentication(String authPluginClassName, String authParamsString) throws PulsarClientException.UnsupportedAuthenticationException {
        return authentication(AuthenticationFactory.create(authPluginClassName, authParamsString));
    }

    @Override
    public ClientBuilder authentication(String authPluginClassName, Map<String, String> authParams) throws PulsarClientException.UnsupportedAuthenticationException {
        return authentication(AuthenticationFactory.create(authPluginClassName, authParams));
    }

    @Override
    public ClientBuilder operationTimeout(int operationTimeout, TimeUnit unit) {
        return null;
    }

    @Override
    public ClientBuilder ioThreads(int numIoThreads) {
        return this;
    }

    @Override
    public ClientBuilder listenerThreads(int numListenerThreads) {
        return null;
    }

    @Override
    public ClientBuilder connectionsPerBroker(int connectionsPerBroker) {
        return null;
    }

    @Override
    public ClientBuilder enableTcpNoDelay(boolean enableTcpNoDelay) {
        return null;
    }

    @Override
    public ClientBuilder enableTls(boolean enableTls) {
        return null;
    }

    @Override
    public ClientBuilder tlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
        return null;
    }

    @Override
    public ClientBuilder allowTlsInsecureConnection(boolean allowTlsInsecureConnection) {
        return null;
    }

    @Override
    public ClientBuilder enableTlsHostnameVerification(boolean enableTlsHostnameVerification) {
        return null;
    }

    @Override
    public ClientBuilder useKeyStoreTls(boolean useKeyStoreTls) {
        return null;
    }

    @Override
    public ClientBuilder sslProvider(String sslProvider) {
        return null;
    }

    @Override
    public ClientBuilder tlsTrustStoreType(String tlsTrustStoreType) {
        return null;
    }

    @Override
    public ClientBuilder tlsTrustStorePath(String tlsTrustStorePath) {
        return null;
    }

    @Override
    public ClientBuilder tlsTrustStorePassword(String tlsTrustStorePassword) {
        return null;
    }

    @Override
    public ClientBuilder tlsCiphers(Set<String> tlsCiphers) {
        return null;
    }

    @Override
    public ClientBuilder tlsProtocols(Set<String> tlsProtocols) {
        return null;
    }

    @Override
    public ClientBuilder statsInterval(long statsInterval, TimeUnit unit) {
        return null;
    }

    @Override
    public ClientBuilder maxConcurrentLookupRequests(int maxConcurrentLookupRequests) {
        return null;
    }

    @Override
    public ClientBuilder maxLookupRequests(int maxLookupRequests) {
        return null;
    }

    @Override
    public ClientBuilder maxLookupRedirects(int maxLookupRedirects) {
        return null;
    }

    @Override
    public ClientBuilder maxNumberOfRejectedRequestPerConnection(int maxNumberOfRejectedRequestPerConnection) {
        return null;
    }

    @Override
    public ClientBuilder keepAliveInterval(int keepAliveInterval, TimeUnit unit) {
        return null;
    }

    @Override
    public ClientBuilder connectionTimeout(int duration, TimeUnit unit) {
        httpClientBuilder.connectTimeout(Duration.ofMillis(unit.toMillis(duration)));
        return this;
    }

    @Override
    public ClientBuilder startingBackoffInterval(long duration, TimeUnit unit) {
        return null;
    }

    @Override
    public ClientBuilder maxBackoffInterval(long duration, TimeUnit unit) {
        return null;
    }

    @Override
    public ClientBuilder clock(Clock clock) {
        return null;
    }

    @Override
    public ClientBuilder proxyServiceUrl(String proxyServiceUrl, ProxyProtocol proxyProtocol) {
        return null;
    }

    @Override
    public ClientBuilder enableTransaction(boolean enableTransaction) {
        return null;
    }
}
