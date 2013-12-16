package com.netflix.nfkafka.metrics;

import com.google.common.collect.Lists;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.nfkafka.ProducerDiscovery;
import kafka.producer.ProducerConfig;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestProducerDiscovery {
    @Test
    public void testStatic() {
        Properties props = new Properties();
        props.setProperty("producer.discovery", "static");
        props.setProperty("metadata.broker.list", "brokerList");
        ProducerConfig config = new ProducerConfig(props);
        ProducerDiscovery discovery = new ProducerDiscovery(config, mock(DiscoveryClient.class));

        assertEquals(discovery.getBrokerList(), "brokerList");
    }

    @Test
    public void testEureka() {
        Properties props = new Properties();
        props.setProperty("producer.discovery", "eureka");
        props.setProperty("metadata.broker.list", "vipAddress:9090");
        ProducerConfig config = new ProducerConfig(props);

        DiscoveryClient discoveryClient = mock(DiscoveryClient.class);
        InstanceInfo info0 = mock(InstanceInfo.class);
        doReturn("host0").when(info0).getHostName();
        InstanceInfo info1 = mock(InstanceInfo.class);
        doReturn("host1").when(info1).getHostName();
        doReturn(Lists.newArrayList(info0, info1)).when(discoveryClient).getInstancesByVipAddress("vipAddress", false);
        ProducerDiscovery discovery = new ProducerDiscovery(config, discoveryClient);

        assertEquals(discovery.getBrokerList(), "host0:9090,host1:9090");
    }

    @Test(expected=RuntimeException.class)
    public void testEurekaFormatException() {
        Properties props = new Properties();
        props.setProperty("producer.discovery", "eureka");
        props.setProperty("metadata.broker.list", "vipAddress");
        ProducerConfig config = new ProducerConfig(props);

        DiscoveryClient discoveryClient = mock(DiscoveryClient.class);
        InstanceInfo info0 = mock(InstanceInfo.class);
        doReturn("host0").when(info0).getHostName();
        InstanceInfo info1 = mock(InstanceInfo.class);
        doReturn("host1").when(info1).getHostName();
        doReturn(Lists.newArrayList(info0, info1)).when(discoveryClient).getInstancesByVipAddress("vipAddress", false);
        ProducerDiscovery discovery = new ProducerDiscovery(config, discoveryClient);
        discovery.getBrokerList();
    }
}
