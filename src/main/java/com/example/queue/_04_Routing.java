package com.example.queue;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;


public class _04_Routing {

    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        ReceiveLogs receiveLogs1 = new ReceiveLogs("ERROR", new SEVERITY[] {SEVERITY.ERROR});
        receiveLogs1.run();
        ReceiveLogs receiveLogs2 = new ReceiveLogs("WARN&INFO", new SEVERITY[] {SEVERITY.INFO, SEVERITY.WARN});
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
                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
                System.out.println(" [EmitLog ready] Enter message. To exit, type EXIT");
                Scanner scanner = new Scanner(System.in);
                while (true) {
                    String cmd = scanner.nextLine().strip();
                    if ("EXIT".equals(cmd)) {
                        break;
                    }
                    if ("BOOM".equals(cmd)) {
                        for (int i = 1; i < 50; i++) {
                            SEVERITY s = SEVERITY.values()[i % 3];
                            String msg = "Message " + i;
                            channel.basicPublish(EXCHANGE_NAME, s.getValue(), null, msg.getBytes("UTF-8"));
                            System.out.println(" [EmitLog] Sent '" + s.getValue() + "' '" + msg + "'");
                        }
                        continue;
                    }
                    String msg[] = cmd.split(":");
                    SEVERITY s = SEVERITY.fromValue(msg[0]);
                    if (s == null) {
                        System.out.println("Allow: " + Arrays.toString(SEVERITY.values()));
                        continue;
                    }
                    channel.basicPublish(EXCHANGE_NAME, s.getValue(), null, msg[1].getBytes("UTF-8"));
                    System.out.println(" [EmitLog] Sent '" + s.getValue() + "' '" + msg[1] + "'");
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

    static enum SEVERITY {

        ERROR("error"), WARN("warn"), INFO("info");

        private String value;

        private SEVERITY(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static SEVERITY fromValue(String value) {
            for (SEVERITY s : SEVERITY.values()) {
                if (s.getValue().equalsIgnoreCase(value)) {
                    return s;
                }
            }
            return null;
        }
    }

    static class ReceiveLogs implements Runnable {

        private String name;
        private SEVERITY[] severity;

        public ReceiveLogs(String name, SEVERITY[] severity) {
            this.name = name;
            this.severity = severity;
        }

        @Override
        public void run() {
            try {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                final Connection connection = factory.newConnection();
                final Channel channel = connection.createChannel();
                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
                String queueName = channel.queueDeclare().getQueue();
                for (SEVERITY severity : this.severity) {
                    channel.queueBind(queueName, EXCHANGE_NAME, severity.getValue());
                }
                System.out.println(" [ReceiveLogs " + name + "] Waiting for messages.");
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println(" [ReceiveLogs " + name + "] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
                };
                channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
                });
            } catch (IOException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
