package com.netflix;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.DiscoveryManager;

public class ProducerDiscovery {
    public static String getBrokerList(String vipAddress) {
        StringBuilder sb = new StringBuilder();
        for (InstanceInfo i : DiscoveryManager.getInstance().getDiscoveryClient().getInstancesByVipAddress(vipAddress, false)) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(i.getHostName()).append(':').append(i.getPort());
        }

        return sb.toString();
    }
}