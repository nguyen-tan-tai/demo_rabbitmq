package com.example;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;


public class _03_PublishSubscribe {

    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        ReceiveLogs receiveLogs1 = new ReceiveLogs("One");
        receiveLogs1.run();
        ReceiveLogs receiveLogs2 = new ReceiveLogs("Two");
        receiveLogs2.run();
        EmitLog emitLog = new EmitLog();
        emitLog.run();
    }

    static class EmitLog implements Runnable {

        @Override
        public void run() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                    Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
                System.out.println(" [EmitLog ready] Enter message. To exit, type EXIT");
                Scanner scanner = new Scanner(System.in);
                while (true) {
                    String cmd = scanner.nextLine().strip();
                    if ("EXIT".equals(cmd)) {
                        break;
                    }
                    if ("BOOM".equals(cmd)) {
                        for (int i = 1; i < 1000000000; i *= 10) {
                            String msg = "Message " + i;
                            channel.basicPublish(EXCHANGE_NAME, "", null, msg.getBytes("UTF-8"));
                            System.out.println(" [EmitLog] Sent '" + msg + "'");
                        }
                        continue;
                    }
                    channel.basicPublish(EXCHANGE_NAME, "", null, cmd.getBytes("UTF-8"));
                    System.out.println(" [EmitLog] Sent '" + cmd + "'");
                }
                scanner.close();
            } catch (IOException | TimeoutException e) {
                throw new RuntimeException(e);
            } finally {
                System.out.println("System terminated!");
                System.exit(0); // Terminate all threads
            }
        }
    }

    static class ReceiveLogs implements Runnable {

        private String name;

        public ReceiveLogs(String name) {
            this.name = name;
        }

        @Override
        public void run() {
            try {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                final Connection connection = factory.newConnection();
                final Channel channel = connection.createChannel();
                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, EXCHANGE_NAME, "");
                System.out.println(" [ReceiveLogs " + name + "] Waiting for messages.");
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    try {
                        Thread.sleep(new Random().nextInt(2) * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(" [ReceiveLogs " + name + "] Received '" + message + "'");
                };
                channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
                });
            } catch (IOException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
