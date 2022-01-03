package kafka;

import java.io.IOException;
import java.util.Scanner;

// Handle starting producer or consumer
public class RunProducer {
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        /*if(args.length < 3) {
            usage();
        }*/
        // Get the brokers
        String brokers = "localhost:9092";
        String topicName = "INPUT_XML_FILES_ISFDB";
        //topicXMLFilesPath
        //topicKeywords
        //topicResults
        /*
        for (int i=0;i<5000000;i++){
            Admin.deleteTopics("topic"+i);
        }*/
        String conteudo ="1";

        while(!conteudo.equals("0")){
            System.out.println("Conteúdo:");
            conteudo=sc.next();

            Producer producer = new Producer();
            producer.produce(topicName,conteudo);
        }
        System.exit(0);
    }
    // Display usage
    public static void usage() {
        System.out.println("Usage:");
        System.out.println("kafka-example.jar <producer|consumer|describe|create|delete> <topicName> brokerhosts [groupid]");
        System.exit(1);
    }
}