package kz.luc.twittertokafkaservice.runner;

import org.apache.hc.core5.http.ParseException;

import java.io.IOException;
import java.net.URISyntaxException;

public interface StreamRunner {
    void start() throws IOException, URISyntaxException, ParseException;
}
