package ccs.pulsar.perform;



import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarProducer {
    private static final Logger log = LoggerFactory.getLogger(PulsarProducer.class);

    // ----- static methods -------------------------------------------------

    public static void main(String[] asArgs) throws Exception {
        String topic = System.getProperty("ccs.perform.topic", "test");
        String key = System.getProperty("ccs.perform.key", "defaultkey");
        long loop_ns = 5_000_000_000L; // ns = 5s
        int iter = Integer.valueOf(System.getProperty("ccs.perform.iterate", "20"));

        LatencyMeasurePingSerializer serializer = new LatencyMeasurePingSerializer();

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
        Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .create();


        try {
            int seq = 0;
            for( int i=0 ; i != iter ; i++ ) {
                int cnt =0;
                long st = System.nanoTime();
                long et = 0;
                while( (et = System.nanoTime()) - st < loop_ns) {
                    try {
                        producer.send(serializer.serialize(null, new LatencyMeasurePing(seq)));
                        producer.flush();
                        seq++;
                        cnt++;
                    }catch(IllegalStateException e) {
                        // NOTE: publishのjavadocに従い、再接続時のバッファオーバーフロー対処
                        log.debug("publish failed.");
                        TimeUnit.MILLISECONDS.sleep(100);
                    }
                }

                log.info("{}: {} ns. {} times. {} ns/op", topic, et-st, cnt, (et-st)/(double)cnt);
            }
        }catch(Throwable th) {
            log.error("occ exception.", th);

        }finally {
            client.close();
        }

    }
}
