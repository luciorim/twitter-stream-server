package kz.luc.twitterserver.twitter.to.kafka.service;

import kz.luc.twitterserver.config.TwitterToKafkaServiceConfigData;
import kz.luc.twitterserver.twitter.to.kafka.service.runner.StreamRunner;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;

@SpringBootApplication
@RequiredArgsConstructor
@ComponentScan(basePackages = "kz.luc.twitterserver")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final StreamRunner streamRunner;

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
    }


    @Override
    public void run(String... args) throws Exception {
        logger.info("Application starts...");
        logger.info(Arrays.toString(twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0])));
        streamRunner.start();
    }
}
