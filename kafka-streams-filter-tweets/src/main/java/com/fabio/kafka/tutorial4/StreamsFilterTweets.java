package com.fabio.kafka.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    public static void main(String[] args) {
        // create properties
        Properties prop = new Properties();
        prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        prop.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        prop.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // crreate topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopics = streamsBuilder.stream("twitter_topic");
        KStream<String, String> filteredStream = inputTopics.filter(
                (k, jsonTweets) -> extractUserFollwersInTweet(jsonTweets) > 10000
                // filter for tweets which has a user of over 10000 followers
        );
        filteredStream.to("important_tweets");

        // build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), prop);

        // start our streams application
        kafkaStreams.start();
    }
    private static JsonParser jsonParser = new JsonParser();
    private static Integer extractUserFollwersInTweet(String tweetJson) {
        try {
           return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }
}
