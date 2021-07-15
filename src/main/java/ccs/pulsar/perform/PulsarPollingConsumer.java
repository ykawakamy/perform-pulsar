package ccs.pulsar.perform;



import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ccs.perform.util.PerformHistogram;
import ccs.perform.util.PerformSnapshot;
import ccs.perform.util.SequencialPerformCounter;

public class PulsarPollingConsumer {
    /** ロガー */
    private static final Logger log = LoggerFactory.getLogger(PulsarPollingConsumer.class);

    public static void main(String[] args) throws Exception {
        String topic = System.getProperty("ccs.perform.topic", "test");
        String subscription = System.getProperty("ccs.perform.sub", "default");
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
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .subscriptionName(subscription)
                .subscribe();

        try {
            // トピックを指定してメッセージを送信する

            for( int i=0 ; i != iter ; i++ ) {
                long st = System.nanoTime();
                long et = 0;


                while( (et = System.nanoTime()) - st < loop_ns) {
                    Message<byte[]> msg = consumer.receive(100, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        byte[] data = msg.getData();
                        LatencyMeasurePing ping = serializer.deserialize("", data);
                        pc.perform(ping.getSeq());
                        long latency = ping.getLatency();
                        pc.addLatency(latency);
                        hist.increament(latency);
                    }
                }

                PerformSnapshot snap = pc.reset();
                log.info("{}: {} op, {} errors, {} ns/op, latency: {} ms/op", topic, snap.getPerform(), snap.getErr(), snap.getElapsedPerOperation(et-st), snap.getLatencyPerOperation() );
            }
        } catch( Throwable th ) {
            th.printStackTrace();
        } finally {
            client.close();
        }
    }

}
