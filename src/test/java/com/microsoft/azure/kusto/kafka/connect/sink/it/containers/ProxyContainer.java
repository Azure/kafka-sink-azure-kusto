package com.microsoft.azure.kusto.kafka.connect.sink.it.containers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

public class ProxyContainer extends GenericContainer<ProxyContainer> {
    public static final String PROXY_IMAGE_NAME = "ubuntu/squid";
    public static final int PROXY_PORT = 3128;

    public ProxyContainer() {
        super(PROXY_IMAGE_NAME + ":latest");
        waitingFor(Wait.forListeningPort());
        withExposedPorts(PROXY_PORT);
    }

    public ProxyContainer withNetwork(Network network) {
        super.withNetwork(network).withExposedPorts(PROXY_PORT);
        return self();
    }
}
