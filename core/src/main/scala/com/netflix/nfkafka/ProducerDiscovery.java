package com.netflix.nfkafka;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.DiscoveryClient;
import kafka.producer.ProducerConfig;

import java.util.List;

public class ProducerDiscovery {
    private final ProducerConfig config;
    private final DiscoveryClient discoveryClient;

    public ProducerDiscovery(
            ProducerConfig config,
            DiscoveryClient discoveryClient) {
        this.config = config;
        this.discoveryClient = discoveryClient;
    }

    public String getBrokerList() {
        if (config.brokerDiscoveryMode().equals("eureka")) {
            String[] host_port = config.brokerList().split(":");
            if (host_port.length != 2) {
                throw new RuntimeException("vipAddress should be with the port");
            }
            StringBuilder sb = new StringBuilder();
            List<InstanceInfo> infoList = discoveryClient.getInstancesByVipAddress(host_port[0], false);
            int count = 0;
            for (InstanceInfo i : infoList) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append(i.getHostName()).append(':').append(host_port[1]);
                ++count;
                if (count == 3) {
                    break;
                }
            }

            return sb.toString();
        } else {
            return config.brokerList();
        }
    }
}