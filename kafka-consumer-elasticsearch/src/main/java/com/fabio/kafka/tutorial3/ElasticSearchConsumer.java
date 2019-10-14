package com.fabio.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));

            Integer recordCount = records.count();

            logger.info("Received: " + recordCount + " records.");

            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {
                // 2 strategies
                // kafka generic ID:
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // twitter feed specific id
                try{
                    String id = extractIdFromTweet(record.value());

                    // where we insert data into elasticsearch
                    String jsonString = record.value();

                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id // this is to make our consumer idempotent
                    ).source(jsonString, XContentType.JSON);

                    bulkRequest.add(indexRequest); // we add to our bulk request (takes no time)
                } catch (NullPointerException e) {
                    logger.warn("Skiping bad data: ", record.value());
                }
            }

            if (recordCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //client.close();
    }
    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweet(String tweetJson) {
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static RestHighLevelClient createClient(){
        String hostname = ".ap-southeast-2.bonsaisearch.net";
        String username = "";
        String password = "";

        // do not do if you run local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(hostname, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });
        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapServers = "localhost:9092";
        String groupId = "kafka-demo-elasticsearch";

        // create consumer configs
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        // subscribe to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }
}
