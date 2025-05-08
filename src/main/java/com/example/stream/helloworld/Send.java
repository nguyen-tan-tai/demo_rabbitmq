package com.example.stream.helloworld;

import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Producer;
import java.io.IOException;

public class Send {

    public static void main(String[] args) throws IOException {
        Environment environment = Environment.builder().build();
        String stream = "hello-java-stream";
        environment.streamCreator().stream(stream).maxLengthBytes(ByteCapacity.GB(5)).create();
        Producer producer = environment.producerBuilder().stream(stream).build();
        for (int i = 1; i < 1000000000; i *= 10) {
            String msg = "Message " + i;
            producer.send(producer.messageBuilder().addData(msg.getBytes()).build(), null);
            System.out.println(" [Sender] Sent '" + msg + "'");
        }
        producer.close();
        environment.close();
    }
}
