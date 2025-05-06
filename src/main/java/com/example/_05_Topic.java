package com.example;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;
import java.util.random.RandomGenerator;


public class _05_Topic {

    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        ReceiveLogs kernLogs = new ReceiveLogs("KERN", new String[] {"kern.*"});
        kernLogs.run();
        ReceiveLogs criticalLogs = new ReceiveLogs("CRITICAL", new String[] {"*.critical"});
        criticalLogs.run();
        ReceiveLogs allLogs = new ReceiveLogs("ALL", new String[] {"#"});
        allLogs.run();
        EmitLog emitLog = new EmitLog();
        emitLog.run();
    }

    public static class EmitLog implements Runnable {

        @Override
        public void run() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                    Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
                System.out.println(" [EmitLog ready] Enter message. To exit, type EXIT");
                Scanner scanner = new Scanner(System.in);
                while (true) {
                    String cmd = scanner.nextLine().strip();
                    if ("EXIT".equals(cmd)) {
                        break;
                    }
                    if ("BOOM".equals(cmd)) {
                        for (int i = 1; i < 50; i++) {
                            String routingKey = Arrays.asList("kern." + i, i + ".critical", "#").get(i % 3);
                            String message = "Message " + routingKey;
                            channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
                            System.out.println(" [EmitLog] Sent '" + routingKey + "' '" + message + "'");
                        }
                        continue;
                    }
                    String msg[] = cmd.split("|");
                    String routingKey = msg[0];
                    String message = msg[1];
                    channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
                    System.out.println(" [EmitLog] Sent '" + routingKey + "' '" + message + "'");
                }
                scanner.close();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                System.out.println("System terminated!");
                System.exit(0); // Terminate all threads
            }
        }
    }

    static class ReceiveLogs implements Runnable {

        private String name;
        private String[] topics;

        public ReceiveLogs(String name, String[] severity) {
            this.name = name;
            this.topics = severity;
        }

        @Override
        public void run() {
            try {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                final Connection connection = factory.newConnection();
                final Channel channel = connection.createChannel();
                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
                String queueName = channel.queueDeclare().getQueue();
                for (String topic : this.topics) {
                    channel.queueBind(queueName, EXCHANGE_NAME, topic);
                }
                System.out.println(" [ReceiveLogs " + name + "] Waiting for messages.");
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println(" [ReceiveLogs " + name + "] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
                    try {
                        Thread.sleep(RandomGenerator.getDefault().nextInt(1000));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                };
                channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
                });
            } catch (IOException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
