package com.netflix.nfkafka;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.DiscoveryManager;

public class ProducerDiscovery {
    public static String getBrokerList(String vipAddress) {
        String[] host_port = vipAddress.split(":");
        if (host_port.length != 2) {
            throw new RuntimeException("vipAddress should be with the port");
        }
        StringBuilder sb = new StringBuilder();
        for (InstanceInfo i : DiscoveryManager.getInstance().getDiscoveryClient().getInstancesByVipAddress(host_port[0], false)) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(i.getHostName()).append(':').append(host_port[1]);
        }

        return sb.toString();
    }
}