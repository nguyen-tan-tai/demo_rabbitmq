package com.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.MessageProperties;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;


public class WorkQueues {

    private final static String TASK_QUEUE_NAME = "task_queue";


    public static void main(String[] args) throws IOException, TimeoutException {
        Receiver receiver1 = new Receiver("One");
        receiver1.run();
        Receiver receiver2 = new Receiver("Two");
        receiver2.run();
        Sender sender = new Sender();
        sender.run();
    }

    static class Sender implements Runnable {

        @Override
        public void run() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                    Channel channel = connection.createChannel()) {
                boolean durable = true;
                boolean exclusive = false;
                boolean autoDelete = false;
                channel.queueDeclare(TASK_QUEUE_NAME, durable, exclusive, autoDelete, null);
                System.out.println(" [Sender ready] Enter message. To exit, type EXIT");
                Scanner scanner = new Scanner(System.in);
                while (true) {
                    String cmd = scanner.nextLine().strip();
                    if ("EXIT".equals(cmd)) {
                        break;
                    }
                    channel.basicPublish("", TASK_QUEUE_NAME,
                            MessageProperties.PERSISTENT_TEXT_PLAIN,
                            cmd.getBytes("UTF-8"));
                    System.out.println(" [Sender] Sent '" + cmd + "'");
                }
                scanner.close();
            } catch (IOException | TimeoutException e) {
                throw new RuntimeException();
            } finally {
                System.out.println("System terminated!");
                System.exit(0); // Terminate all threads
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
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (
                    final Connection connection = factory.newConnection();
                    final Channel channel = connection.createChannel();) {
                boolean durable = true;
                boolean exclusive = false;
                boolean autoDelete = false;
                channel.queueDeclare(TASK_QUEUE_NAME, durable, exclusive, autoDelete, null);
                System.out.println(" [Receiver " + name + "] Waiting for messages.");

                // This tells RabbitMQ not to give more than one message to a worker at a time.
                // Or, in other words, don't dispatch a new message to a worker until it has processed and acknowledged the previous one.
                // Instead, it will dispatch it to the next worker that is not still busy.
                int prefetchCount = 1;
                channel.basicQos(prefetchCount);
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println(" [Receiver " + name + "] Received '" + message + "'");
                    try {
                        doWork(message);
                    } finally {
                        System.out.println(" [Receiver " + name + "] Done");
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    }
                };
                boolean autoAck = false;
                channel.basicConsume(TASK_QUEUE_NAME, autoAck, deliverCallback, consumerTag -> {
                });
            } catch (IOException | TimeoutException e) {
                throw new RuntimeException();
            }
        }
    }

    private static void doWork(String task) {
        for (char ch : task.toCharArray()) {
            if (ch == '.') {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException _ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
