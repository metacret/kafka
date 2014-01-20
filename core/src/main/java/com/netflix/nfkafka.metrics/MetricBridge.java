package com.netflix.nfkafka.metrics;

import com.yammer.metrics.core.Metric;

public interface MetricBridge {
    void update(Metric metric);
}
