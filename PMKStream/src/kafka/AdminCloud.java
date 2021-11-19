package kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class AdminCloud {

    public static Properties getProperties() {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "cell-1.streaming.sa-saopaulo-1.oci.oraclecloud.com:9092");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"natynlara/oracleidentitycloudservice/natynlara@gmail.com/ocid1.streampool.oc1.sa-saopaulo-1.amaaaaaavwvqtcaaksclwf6o3r5vlkhgemd6dog2nwrlct7wwxdp3etibrbq\" password=\"AUTH_TOKEN\";");

        /*
        // Set how to serialize key/value pairs
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // specify the protocol for Domain Joined clusters
        //properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        */

        return properties;
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

    public static void main(String[] args) {
        try {
            createTopics("teste");
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
