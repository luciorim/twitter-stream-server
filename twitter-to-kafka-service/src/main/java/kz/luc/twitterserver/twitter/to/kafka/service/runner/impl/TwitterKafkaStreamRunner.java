package kz.luc.twitterserver.twitter.to.kafka.service.runner.impl;

import jakarta.annotation.PreDestroy;
import kz.luc.twitterserver.config.TwitterToKafkaServiceConfigData;
import kz.luc.twitterserver.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import kz.luc.twitterserver.twitter.to.kafka.service.runner.StreamRunner;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import java.util.Arrays;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "false")
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger logger  = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private TwitterStream twitterStream;

    @Override
    public void start() {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();
    }

    @PreDestroy
    public void shutdown() {
        if (twitterStream != null) {
            logger.info("Closing twitter stream");
            twitterStream.shutdown();
        }
    }

    private void addFilter() {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        logger.info("Started filtering twitter streams for keywords: {}", Arrays.toString(keywords));
    }
}
