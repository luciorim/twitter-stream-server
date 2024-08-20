package kz.luc.twittertokafkaservice.runner.impl;

import kz.luc.twittertokafkaservice.config.TwitterToKafkaServiceConfigData;
import kz.luc.twittertokafkaservice.runner.StreamRunner;
import lombok.RequiredArgsConstructor;
import org.apache.hc.core5.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@ConditionalOnExpression("${twitter-to-kafka-service.enable-v2-tweets} && not ${twitter-to-kafka-service.enable-mock-tweets}")
public class TwitterV2KafkaStreamRunner implements StreamRunner {

    private static final Logger logger = LoggerFactory.getLogger(TwitterV2KafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final TwitterV2StreamHelper twitterV2StreamHelper;

    @Override
    public void start() {
        String bearerToken = twitterToKafkaServiceConfigData.getTwitterV2BearerToken();
        if (bearerToken != null) {
            try {
                twitterV2StreamHelper.setupRules(bearerToken, getRules());
                twitterV2StreamHelper.connectStream(bearerToken);
            } catch (URISyntaxException | IOException | ParseException e) {
                logger.error("Error streaming tweets: {}", e.getMessage());
                throw new RuntimeException("Error streaming tweets: ", e);
            }
        } else {
            logger.error("Bearer token not set. Please make sure you set TWITTER_BEARER_TOKEN environment variable.");
            throw new RuntimeException("Bearer token not set. Please make sure you set TWITTER_BEARER_TOKEN environment variable.");
        }
    }

    private Map<String, String> getRules() {
        List<String> keywords = twitterToKafkaServiceConfigData.getTwitterKeywords();
        Map<String, String> rules = keywords.stream()
                .collect(Collectors.toMap(keyword -> keyword, "Keyword: %s"::formatted));
        logger.info("Created filter for twitter stream: {}", keywords);
        return rules;
    }
}
