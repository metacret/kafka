package com.netflix.nfkafka.filequeue;

public interface SerDe<K, V> {
    K getKeyFromBytes(byte[] payload);
    V getValueFromBytes(byte[] payload);

    byte[] toBytesKey(K key);
    byte[] toBytesValue(V value);
}
