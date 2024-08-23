package kz.luc.twitterserver.twitter.to.kafka.service.runner;

import org.apache.hc.core5.http.ParseException;
import twitter4j.TwitterException;

import java.io.IOException;
import java.net.URISyntaxException;

public interface StreamRunner {
    void start() throws IOException, URISyntaxException, ParseException, TwitterException;
}
