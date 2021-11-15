package kafka;

import java.util.Scanner;
import java.util.UUID;
import java.io.IOException;

public class RunConsumer {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String brokers = "localhost:9092";

        // Either a groupId was passed in, or we need a random one

        System.out.println("Nome do topico:");
        String topicName=sc.next();

        String groupId = UUID.randomUUID().toString();

        System.out.println("GroupId: "+ groupId);
        Consumer.consume( groupId, topicName);
    }
}
