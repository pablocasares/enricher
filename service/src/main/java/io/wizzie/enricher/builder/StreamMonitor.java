package io.wizzie.enricher.builder;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class StreamMonitor extends Thread {
    private static final Logger log = LoggerFactory.getLogger(StreamMonitor.class);

    KafkaStreams streams;
    Builder builder;

    public StreamMonitor(Builder builder) {
        this.streams = builder.streams;
        this.builder = builder;
    }

    @Override
    public void run() {
        boolean monitoring = true;
        log.info("Start Stream Monitor thread.");

        while (monitoring && !isInterrupted()) {
            if (streams.state().isRunning()) {
                List<Boolean> streamsRunning = streams.localThreadsMetadata()
                        .stream()
                        .map(x -> !x.threadState().equals(StreamThread.State.DEAD.name()))
                        .collect(Collectors.toList());

                log.debug("Check stream threads are running: {}", streamsRunning.toString());
                if (!streamsRunning.contains(true)) {
                    log.info("Detect streams is shutdown, I'm going to close the builder.");
                    try {
                        builder.close();
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }

                    monitoring = false;
                } else {
                    try {
                        Thread.sleep(60000);
                    } catch (InterruptedException e) {
                    }
                }
            }else {
                try {
                    Thread.sleep(60000);
                    log.info("Streams not running. Stream Monitor sleeping.");
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }

        log.info("Shutdown Stream Monitor thread.");
    }
}
