package kafka.exemplebymyself;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class ProducerTwitterWithCallbacks {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String firstTopic = "fisrt_topic";

        final Logger logger = LoggerFactory.getLogger(ProducerTwitterWithCallbacks.class);

        // create producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        List<String> listOfTweets = new TwitterProducer().getTweets();

        for (String tweet : listOfTweets) {
            // create producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("twitter_topic", tweet);
            // send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an execption is thrown
                    if (e == null){
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing " + e);
                    }
                }
            });
        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
