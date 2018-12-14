package io.wizzie.enricher;

import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.enricher.builder.Builder;
import io.wizzie.enricher.logo.LogoPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Enricher {
    private static final Logger log = LoggerFactory.getLogger(Enricher.class);

    public static void main(String args[]) throws Exception {
        LogoPrinter.PrintLogo();
        if (args.length == 1) {
            Config config = new Config(args[0]);
            Builder builder = new Builder(config.clone());

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    builder.close();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
                log.info("Stopped Enricher process.");
            }));


        } else {
            log.error("Execute: java -cp ${JAR_PATH} io.wizzie.enricher.Enricher <config_file>");
        }

    }
}
