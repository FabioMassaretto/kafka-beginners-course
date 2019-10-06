package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = "localhost:9092";
        String firstTopic = "fisrt_topic";

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // create producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        for (int i = 0; i <10; i++){
            // create producer record

            String topic = firstTopic;
            String value = "Hello world > " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            // tips: providing key we guarantee that the data always goes to the same partition
            // every time we run the application
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key); // log the key

            // send data
            producer.send(record, new Callback() {
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
            }).get(); // block the send() to make it synchronous - don't do this in production
        }


        // flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
