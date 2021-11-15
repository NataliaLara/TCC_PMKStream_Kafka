package kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class Admin {
    private static String brokers = "localhost:9092";

    public static String getBrokers(){
        return brokers;
    }
    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        // Set how to serialize key/value pairs
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // specify the protocol for Domain Joined clusters
        //properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        return properties;
    }

    public static void describeTopics( String topicName) throws IOException {
        // Set properties used to configure admin client
        Properties properties = getProperties();

        try (final AdminClient adminClient = KafkaAdminClient.create(properties)) {
            // Make async call to describe the topic.
            final DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(topicName));

            TopicDescription description = describeTopicsResult.values().get(topicName).get();
            System.out.print(description.toString());
        } catch (Exception e) {
            System.out.print("Describe denied\n");
            System.out.print(e.getMessage());
            //throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static void deleteTopics( String topicName) throws IOException {
        // Set properties used to configure admin client
        Properties properties = getProperties();

        try (final AdminClient adminClient = KafkaAdminClient.create(properties)) {
            final DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singleton(topicName));
            deleteTopicsResult.values().get(topicName).get();
            //System.out.print("Topic " + topicName + " deleted");
        } catch (Exception e) {
            System.out.print("Delete Topics denied\n");
            System.out.print(e.getMessage());
            //throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static void createTopics( String topicName) throws IOException {
        // Set properties used to configure admin client
        Properties properties = getProperties();

        try (final AdminClient adminClient = KafkaAdminClient.create(properties)) {
            int numPartitions = 8;
            short replicationFactor = (short)1;
            final NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);

            final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
            createTopicsResult.values().get(topicName).get();
            //System.out.print("Topic " + topicName + " created");
        } catch (Exception e) {
            System.out.print("Create Topics denied\n");
            System.out.print(e.getMessage());
            //throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static class Runner {
        public static void main(String[] args) {
           Stream stream = new Stream();

           stream.inicializaStream();
        }
    }

    public static class Stream {

        public static Properties streamProperties(){
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ic_streams");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            return props;
        }

        public static void runStream(final KafkaStreams streams, final CountDownLatch latch){
            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {

                    streams.close();
                    latch.countDown();
                }
            });

            try {
                streams.start();
                latch.await();

            } catch (Throwable e) {
                System.exit(1);
            }
            System.exit(0);
        }

        public void inicializaStream(){
            final StreamsBuilder builder = new StreamsBuilder();

            KStream<String, String> source = builder.stream("streams-words-input"); //gera os dados
            source.to("streams-pipe-output"); //armazenados

            final Topology topology = builder.build();
            //System.out.println(topology.describe());

            final KafkaStreams streams = new KafkaStreams(topology, streamProperties());
            final CountDownLatch latch = new CountDownLatch(1);

            runStream(streams,latch);
        }

    }
}