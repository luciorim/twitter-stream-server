package kz.luc.twitterserver.twitter.to.kafka.service.runner.impl;

import kz.luc.twitterserver.config.TwitterToKafkaServiceConfigData;
import kz.luc.twitterserver.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import kz.luc.twitterserver.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import kz.luc.twitterserver.twitter.to.kafka.service.runner.StreamRunner;
import lombok.RequiredArgsConstructor;
import org.apache.hc.core5.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
@Primary
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger logger = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private static final Random RANDOM = new Random();

    private static final String[] WORDS = {
            "Lorem", "ipsum", "dolor", "sit", "amet",
            "consectetur", "adipiscing", "elit", "sed", "do",
            "eiusmod", "tempor", "incididunt", "ut", "labore",
            "et", "dolore", "magna", "aliqua", "Ut",
            "enim", "ad", "minim", "veniam", "quis"
    };

    private static final String tweetAsRawJson = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    @Override
    public void start() throws IOException, URISyntaxException, ParseException, TwitterException {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        int maxLength = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        int minLength = twitterToKafkaServiceConfigData.getMockMinTweetLength();
        long sleepMs = twitterToKafkaServiceConfigData.getMockSleepMs();
        
        logger.info("Starting mock stream runner");

        simulateTwitterStream(keywords, minLength, maxLength, sleepMs);
    }

    private void simulateTwitterStream(String[] keywords, int minLength, int maxLength, long sleepMs) {
        Executors.newSingleThreadExecutor().submit(() -> {
            while (true) {
                String formattedTweetAsJson = getFormattedTweet(keywords, minLength, maxLength);
                Status status = TwitterObjectFactory.createStatus(formattedTweetAsJson);
                twitterKafkaStatusListener.onStatus(status);
                sleep(sleepMs);
            }
        });
    }

    private void sleep(long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            throw new TwitterToKafkaServiceException("Error while sleeping for waiting new status");
        }
    }

    private String getFormattedTweet(String[] keywords, int minLength, int maxLength) {
        String[] params = {
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords, minLength, maxLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
        };
        return formatTweetToJson(params);
    }

    private static String formatTweetToJson(String[] params) {
        String tweet = tweetAsRawJson;

        for(int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minLength, int maxLength) {
        StringBuilder tweetBuilder = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxLength - minLength + 1) + minLength;
        return constructRandomTweet(keywords, tweetLength, tweetBuilder);
    }

    private static String constructRandomTweet(String[] keywords, int tweetLength, StringBuilder tweetBuilder) {
        for (int i = 0; i < tweetLength; i++) {
            tweetBuilder.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if (i == tweetLength / 2) {
                tweetBuilder.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweetBuilder.toString().trim();
    }
}
