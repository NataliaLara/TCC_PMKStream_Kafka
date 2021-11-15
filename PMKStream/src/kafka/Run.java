package kafka;

import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;

// Handle starting producer or consumer
public class Run {
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        /*if(args.length < 3) {
            usage();
        }*/
        // Get the brokers
        String brokers = "localhost:9092";
        String topicName = "teste";
        //topicXMLFilesPath
        //topicKeywords
        //topicResults
        /*
        for (int i=0;i<5000000;i++){
            Admin.deleteTopics("topic"+i);
        }*/

        System.out.println("Selecione a opção:");
        System.out.println("producer|consumer|describe(topic)|create(topic)|delete(topic)|deleteall(topic)");
        String opcao = sc.next();
        System.out.println("Nome do topico:");
        topicName=sc.next();



        switch(opcao.toLowerCase()) {
            case "producer":

                System.out.println("Conteúdo:");
                String conteudo=sc.next();

                Producer producer = new Producer();
                producer.produce(topicName,conteudo);
                break;
            case "consumer":
                // Either a groupId was passed in, or we need a random one
                String groupId;
                if(args.length == 4) {
                    groupId = args[3];
                } else {
                    groupId = UUID.randomUUID().toString();
                }
                System.out.println("GroupId: "+ groupId);
                Consumer.consume( groupId, topicName);
                break;
            case "describe":
                Admin.describeTopics(topicName);
                break;
            case "create":
                Admin.createTopics(topicName);
                break;
            case "delete":
                Admin.deleteTopics(topicName);
                break;
            case "deleteall":
                for (int i=0; i<=587;i++){
                    Admin.deleteTopics("topic"+i);
                }
                System.out.println("Tópicos excluídos com sucesso");
                break;
            default:
                usage();
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