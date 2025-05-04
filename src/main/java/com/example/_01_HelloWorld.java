package com.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;


public class _01_HelloWorld {

    private final static String QUEUE_NAME = "hello";


    public static void main(String[] args) throws IOException, TimeoutException {
        Receiver receiver = new Receiver("One");
        receiver.run();
        Sender sender = new Sender();
        sender.run();
    }

    static class Sender implements Runnable {

        @Override
        public void run() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (
                    Connection connection = factory.newConnection();
                    Channel channel = connection.createChannel()) {
                channel.queueDeclare(_01_HelloWorld.QUEUE_NAME, false, false, false, null);
                System.out.println(" [Sender ready] Enter message. To exit, type EXIT");
                Scanner scanner = new Scanner(System.in);
                while (true) {
                    String cmd = scanner.nextLine().strip();
                    if ("EXIT".equals(cmd)) {
                        break;
                    }
                    channel.basicPublish("", _01_HelloWorld.QUEUE_NAME, null, cmd.getBytes());
                    System.out.println(" [Sender] Sent '" + cmd + "'");
                }
                scanner.close();
                System.exit(0);
            } catch (IOException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class Receiver implements Runnable {

        private String name;

        public Receiver(String name) {
            this.name = name;
        }

        @Override
        public void run() {
            try {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();
                channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                System.out.println(" [Receiver " + name + "] Waiting for messages.");
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println(" [Receiver " + name + "] Received '" + message + "'");
                };
                channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
                });
            } catch (IOException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
