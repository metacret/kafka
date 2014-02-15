package com.netflix.nfkafka.filequeue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;
import kafka.producer.KeyedMessage;

import javax.annotation.PreDestroy;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FileBlockingQueue<K, V> extends AbstractQueue<KeyedMessage<K, V>> implements BlockingQueue<KeyedMessage<K, V>> {
    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    private final IBigQueue queue;
    private final SerDe<K, V> serDe;

    public FileBlockingQueue(String fileQueuePath, int gcIntervalInSec, SerDe<K, V> serDe) {
        try {
            queue = new BigQueueImpl(fileQueuePath, "kafkaProducer");
            this.serDe = serDe;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    queue.gc();
                } catch (IOException e) {
                    // ignore exception while gc
                }
            }
        }, gcIntervalInSec, gcIntervalInSec, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void gc() {
        try {
            queue.gc();
        } catch (IOException e) {
            // ignore exception while gc
        }
    }

    @Override
    public KeyedMessage<K, V> poll() {
        if (isEmpty()) {
            return null;
        }
        KeyedMessage<K, V> x = null;
        lock.lock();
        try {
            if (isEmpty() == false) {
                x = deserialize(queue.dequeue());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

        return x;
    }

    @Override
    public KeyedMessage<K, V> peek() {
        if (isEmpty()) {
            return null;
        }
        lock.lock();
        try {
            return deserialize(queue.peek());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(KeyedMessage<K, V> e) {
        Preconditions.checkNotNull(e);
        try {
            queue.enqueue(serialize(e));
            signalNotEmpty();
            return true;
        } catch (IOException ex) {
            return false;
        }
    }

    @Override
    public void put(KeyedMessage<K, V> e) throws InterruptedException {
        offer(e);
    }

    @Override
    public boolean offer(KeyedMessage<K, V> e, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(e);
    }

    @Override
    public KeyedMessage<K, V> take() throws InterruptedException {
        KeyedMessage<K, V> x;
        lock.lockInterruptibly();
        try {
            while (isEmpty()) {
                notEmpty.await();
            }
            x = deserialize(queue.dequeue());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

        return x;
    }

    @Override
    public KeyedMessage<K, V> poll(long timeout, TimeUnit unit) throws InterruptedException {
        KeyedMessage<K, V> x = null;
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (isEmpty()) {
                if (nanos <= 0)
                    return null;
                nanos = notEmpty.awaitNanos(nanos);
            }
            x = deserialize(queue.dequeue());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

        return x;
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super KeyedMessage<K, V>> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super KeyedMessage<K, V>> c, int maxElements) {
        if (c == null)
            throw new NullPointerException("Collection argument should not be null");
        if (c == this)
            throw new IllegalArgumentException("Queue cannot drain to itself");

        lock.lock();
        try {
            int n = Math.min(maxElements, size());
            // count.get provides visibility to first n Nodes
            int i = 0;
            while (i < n) {
                c.add(deserialize(queue.dequeue()));
                ++i;
            }
            return n;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Iterator<KeyedMessage<K, V>> iterator() {
        return new Iterator<KeyedMessage<K, V>>() {

            @Override
            public boolean hasNext() {
                return queue.isEmpty() == false;
            }

            @Override
            public KeyedMessage<K, V> next() {
                try {
                    return deserialize(queue.dequeue());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported, use dequeue()");
            }
        };
    }

    @Override
    public int size() {
        return (int) queue.size();
    }

    private void signalNotEmpty() {
        lock.lock();
        try {
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    private ThreadLocal<ByteArrayOutputStream> outputStream =
            new ThreadLocal<ByteArrayOutputStream>() {
                @Override
                protected ByteArrayOutputStream initialValue() {
                    return new ByteArrayOutputStream();
                }

                @Override
                public ByteArrayOutputStream get() {
                    ByteArrayOutputStream b = super.get();
                    b.reset();
                    return b;
                }
            };

    @VisibleForTesting
    protected byte[] serialize(KeyedMessage<K, V> message) {
        ByteArrayDataOutput out = new ByteArrayDataOutputStream(outputStream.get());
        out.writeUTF(message.topic());
        writeTo(out, serDe.toBytesKey(message.key()));
        writeTo(out, serDe.toBytesValue(message.message()));

        return out.toByteArray();
    }

    private void writeTo(ByteArrayDataOutput out, byte[] encoded) {
        if (encoded != null && encoded.length > 0) {
            out.writeInt(encoded.length);
            out.write(encoded);
        } else {
            out.writeInt(0);
        }
    }

    @VisibleForTesting
    protected KeyedMessage<K, V> deserialize(byte[] buffer) {
        try {
            DataInput input = ByteStreams.newDataInput(buffer);
            String topic = input.readUTF();
            K key;
            V value;
            key = readKeyFrom(input);
            value = readValueFrom(input);
            return new KeyedMessage<K, V>(topic, key, value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private K readKeyFrom(DataInput input) throws IOException {
        int length = input.readInt();
        if (length > 0) {
            byte[] payload = new byte[length];
            input.readFully(payload);

            return serDe.getKeyFromBytes(payload);
        } else {
             return null;
        }
    }

    private V readValueFrom(DataInput input) throws IOException {
        int length = input.readInt();
        if (length > 0) {
            byte[] payload = new byte[length];
            input.readFully(payload);

            return serDe.getValueFromBytes(payload);
        } else {
            return null;
        }
    }
}
