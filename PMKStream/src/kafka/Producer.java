package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class Producer
{
    private static long startTime;
    private static long endTime;
    private static long totalTime;
    private static KafkaProducer<String, String> producer;

    public static long getTotalTime(){
        return totalTime;
    }

    public Producer(){
        this.totalTime=0;
        // Set properties used to configure the producer
        Properties properties = new Properties();
        // Set the brokers (bootstrap servers)
        properties.setProperty("bootstrap.servers", Admin.getBrokers());
        // Set how to serialize key/value pairs
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        // specify the protocol for Domain Joined clusters
        //properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        producer = new KafkaProducer<>(properties);
    }

    public static void produce( String topicName,String conteudo) throws IOException
    {
        startTime= System.currentTimeMillis();

        try
        {
            producer.send(new ProducerRecord<String, String>(topicName, conteudo)).get();
            //producer.wait();
            //System.out.println(conteudo);
        }
        catch (Exception ex)
        {
            System.out.print(ex.getMessage());
            throw new IOException(ex.toString());
        }
        endTime= System.currentTimeMillis();

        totalTime= totalTime + endTime-startTime;
        //System.out.println(totalTime);
    }
}