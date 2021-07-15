package ccs.pulsar.perform;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ccs.perform.util.PerformHistogram;
import ccs.perform.util.PerformSnapshot;
import ccs.perform.util.SequencialPerformCounter;

public class PulsarAsyncConsumer {
    /** ロガー */
    private static final Logger log = LoggerFactory.getLogger(PulsarAsyncConsumer.class);

    public static void main(String[] args) throws Exception {
        String topic = System.getProperty("ccs.perform.topic", "test");
        String subscription = System.getProperty("ccs.perform.sub", PulsarAsyncConsumer.class.getName());
        String key = System.getProperty("ccs.perform.key", "defaultkey");
        long loop_ns = 5_000_000_000L; // ns = 5s
        int iter = Integer.valueOf(System.getProperty("ccs.perform.iterate", "20"));

        PerformHistogram hist = new PerformHistogram();
        hist.addShutdownHook();
        SequencialPerformCounter pc = new SequencialPerformCounter();

        LatencyMeasurePingDeserializer serializer = new LatencyMeasurePingDeserializer();

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        AtomicBoolean isWork = new AtomicBoolean(true);
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .subscriptionName(subscription)
                .subscribe();

        try {
            AtomicInteger latch = new AtomicInteger(0);

            for (int i = 0; i != iter; i++) {
                long st = System.nanoTime();
                long et = 0;

                while ((et = System.nanoTime()) - st < loop_ns) {
                    // receiveAsyncでFutureがたまりすぎるので制限
                    if( latch.get() < 100000) {
                        latch.incrementAndGet();
                        consumer.receiveAsync().thenAccept((msg)->{
                            if (msg != null) {
                                byte[] data = msg.getData();
                                LatencyMeasurePing ping = serializer.deserialize("", data);
                                pc.perform(ping.getSeq());
                                long latency = ping.getLatency();
                                pc.addLatency(latency);
                                hist.increament(latency);
                            }
    //                        acknowledge(consumer, msg);
                            latch.decrementAndGet();
                        });
                    }else {
                        Thread.yield();
                    }

                }

                PerformSnapshot snap = pc.reset();
                log.info("{}: {} op, {} errors, {} ns/op, latency: {} ms/op", topic, snap.getPerform(), snap.getErr(),
                        snap.getElapsedPerOperation(et - st), snap.getLatencyPerOperation());
            }
        } catch (Throwable th) {
            th.printStackTrace();
        } finally {
            isWork.set(false);
            client.close();
        }
    }

    private static void acknowledge(Consumer<byte[]> consumer, Message<byte[]> msg) {
        try {
            consumer.acknowledge(msg);
        } catch (PulsarClientException e) {
            // TODO 自動生成された catch ブロック
            e.printStackTrace();
        }
    }

}
