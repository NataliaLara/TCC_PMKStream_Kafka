package main;

import kafka.Consumer;

public class RunAllMKStream {
    public static void main(String[] args) {
        Consumer.consumePKMStream("INPUT_XML_FILES");
    }
}
