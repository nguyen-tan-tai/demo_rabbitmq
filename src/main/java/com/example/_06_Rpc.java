package com.example;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.Closeable;
import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


public class _06_Rpc {

    private static final String RPC_QUEUE_NAME = "rpc_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        Server server = new Server();
        server.run();
        try (Client client = new Client("One")) {
            System.out.println(" [Client] Ready to accept message. To exit, type EXIT");
            Scanner scanner = new Scanner(System.in);
            while (true) {
                String cmd = scanner.nextLine().strip();
                if ("EXIT".equals(cmd)) {
                    break;
                }
                if ("BOOM".equals(cmd)) {
                    for (int i = 1; i < 10; i++) {
                        String message = "message " + i;
                        client.call(message);
                    }
                    continue;
                }
                client.call(cmd);
            }
            scanner.close();
            System.exit(0);

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    static class Server implements Runnable {

        @Override
        public void run() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try {
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();
                channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
                channel.queuePurge(RPC_QUEUE_NAME);
                channel.basicQos(1);
                System.out.println(" [Server] Awaiting RPC requests");
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                            .correlationId(delivery.getProperties().getCorrelationId())
                            .build();
                    String response = "";
                    try {
                        String message = new String(delivery.getBody(), "UTF-8");
                        response = message + " ^.^";
                        System.out.println(" [Server] Accept " + message + ", response = " + response);
                    } finally {
                        channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    }
                };
                channel.basicConsume(RPC_QUEUE_NAME, false, deliverCallback, (consumerTag -> {
                }));
            } catch (IOException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class Client implements Closeable {

        private String name;
        private Connection connection;
        private Channel channel;

        public Client(String name) throws IOException, TimeoutException {
            this.name = name;
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            connection = factory.newConnection();
            channel = connection.createChannel();
        }

        public String call(String message) {
            try {
                final String correlationId = UUID.randomUUID().toString();
                String replyQueueName = channel.queueDeclare().getQueue();
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .correlationId(correlationId)
                        .replyTo(replyQueueName)
                        .build();
                channel.basicPublish("", RPC_QUEUE_NAME, props, message.getBytes("UTF-8"));
                final CompletableFuture<String> response = new CompletableFuture<>();
                String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
                    if (delivery.getProperties().getCorrelationId().equals(correlationId)) {
                        response.complete(new String(delivery.getBody(), "UTF-8"));
                    }
                }, consumerTag -> {
                });
                System.out.println(" [Client " + this.name + "] call " + message);
                String result = response.get();
                System.out.println(" [Client " + this.name + "] Server return " + result + " for parameter '" + message + "'");
                channel.basicCancel(ctag);
                return result;
            } catch (IOException | InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws IOException {
            connection.close();
        }
    }
}
