package main;

import kafka.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RunAllMKStream implements Runnable{

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private KafkaConsumer<String, String> consumer;
    private static String topicName;
    private static String baseName = "isfdb";

    public RunAllMKStream(KafkaConsumer<String, String> consumer, String topicName) {
        this.consumer = consumer;
        this.topicName = topicName;
    }

    @Override
    public void run() {
        try {
            consumer = createConsumerProperties();
            consumer.subscribe(Collections.singleton(topicName));
            ConsumerRecords<String, String> records;
            while (!closed.get()) {
                synchronized (consumer) {
                    records = consumer.poll(100);
                }
                for (ConsumerRecord<String, String> tmp : records) {
                    System.out.println(tmp.value());
                    String [] args = new String[6];
                    args[0] = "isfdb";
                    args[1] = "50000"; //50000 -> numero de queries
                    args[2] = "1"; //stacks
                    args[3] = "6";
                    args[5] = tmp.value()+"/"; //diretorio
                    RunMKStreamParallel.main(args);
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            System.out.println(e);
            //if (!closed.get()) throw e;
        }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    public static void main(String[] args) {
        //consumePKMStream("INPUT_XML_FILES_ISFDB");

        try {
            startVersaoFuture();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static void startVersaoAnterior(){
        KafkaConsumer<String, String> kafkaConsumer = createConsumerProperties();
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        executorService.execute(new RunAllMKStream(kafkaConsumer, "INPUT_XML_FILES_ISFDB"));
        executorService.shutdown();
    }

    private static void startVersaoFuture() throws ExecutionException, InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(10);
        String topicName="INPUT_XML_FILES_ISFDB";

        // Create a consumer
        KafkaConsumer<String, String> consumer = createConsumerProperties();

        // Subscribe to the 'test' topic
        consumer.subscribe(Arrays.asList(topicName));
        // Loop until ctrl + c
        int count = 0;
        long start = System.nanoTime();
        while(true) {
            // Poll for records
            ConsumerRecords<String, String> records = consumer.poll(200);
            // Did we get any?
            if (records.count() == 0) {
                // timeout/nothing to read
            } else {
                // Yes, loop over records
                for(ConsumerRecord<String, String> record: records) {
                    // Display record and count
                    start = System.nanoTime();
                    count += 1;
                    System.out.println( count + ": " + record.value());
                    //Runnable PMKStream = new RunMKStreamParallel(record.value());
                    Callable<String> c1 = getData(record.value(), 5000);
                    Future<String> f1 = pool.submit(c1);
                    System.out.println(f1.get());
                    long end = System.nanoTime();
                    System.out.println("Tempo decorrido (segundos) = "
                            + ((end - start)/1.0E9));

                }
                pool.shutdown();
            }
        }

    }

    public static Callable<String> getData(String filePath, final int time) {
        return new Callable<String>() {
            @Override
            public String call() throws Exception {
                Thread.sleep(time);
                String [] args = new String[6];
                args[0] = "isfdb";
                args[1] = "50000"; //50000 -> numero de queries
                args[2] = "1"; //stacks
                args[3] = "6";
                args[5] = filePath+"/"; //diretorio
                RunMKStreamParallel.main(args);
                return "TESTE-" + filePath;
            }
        };
    }

    private static int consumePKMStream( String topicName) {


        // Create a consumer
        KafkaConsumer<String, String> consumer = createConsumerProperties();

        // Subscribe to the 'test' topic
        consumer.subscribe(Arrays.asList(topicName));
        // Loop until ctrl + c
        int count = 0;
        while(true) {
            // Poll for records
            ConsumerRecords<String, String> records = consumer.poll(200);
            // Did we get any?
            if (records.count() == 0) {
                // timeout/nothing to read
            } else {
                // Yes, loop over records
                for(ConsumerRecord<String, String> record: records) {
                    // Display record and count
                    count += 1;
                    System.out.println( count + ": " + record.value());
                    Runnable PMKStream = new RunMKStreamParallel(record.value());
                }
            }
        }
    }

    private static KafkaConsumer<String, String> createConsumerProperties(){
        String groupId = UUID.randomUUID().toString();

        // Configure the consumer
        Properties properties = new Properties();
        // Point it to the brokers
        properties.setProperty("bootstrap.servers", Admin.getBrokers());
        // Set the consumer group (all consumers must belong to a group).
        properties.setProperty("group.id", groupId);
        // Set how to serialize key/value pairs
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // When a group is first created, it has no offset stored to start reading from. This tells it to start
        // with the earliest record in the stream.
        properties.setProperty("auto.offset.reset","earliest");

        // specify the protocol for Domain Joined clusters
        //properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        return new KafkaConsumer<>(properties);

    }
}
