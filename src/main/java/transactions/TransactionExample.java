package transactions;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.util.concurrent.TimeoutException;

public class TransactionExample {
    private static final String QUEUE_NAME = "q.transactions-example";
    static class SampleProducer extends Thread {
        public void run() {
            System.out.println("--> Running producer");
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            factory.setPort(5672);
            try(Connection connection = factory.newConnection();
                Channel channel = connection.createChannel()) {
                channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                channel.queuePurge(QUEUE_NAME);

                channel.txSelect();
                for (int i = 1; i <= 5; i++) {
                    String message = "Hello world " + i;
                    if(i == 5) {
                        message = "Final Message" + i;
                    }
                    channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println(" [x] Sent '" + message + "'");
                    sleep(2000);
                }
                channel.txCommit();
            } catch (TimeoutException | IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    static class SampleConsumer extends Thread {
        public void run() {
            try {
                sleep(1000);
                System.out.println("--> Running Consumer");

                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                factory.setPort(5672);

                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();

                channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                System.out.println(" [*] Waiting for messages...");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    channel.txSelect();
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    System.out.println(" [x] Received '" + message + "'");
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                    if(message.startsWith("Final Message")) {
                        channel.txRollback();
                        channel.basicCancel("");
                    }
                };
                channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });

                sleep(Long.MAX_VALUE);
            } catch (InterruptedException | IOException | TimeoutException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws InterruptedException {
        SampleProducer producer = new SampleProducer();
        producer.start();

        SampleConsumer consumer = new SampleConsumer();
        consumer.start();

        producer.join();
        consumer.join();

        System.out.println("Done");
    }
}
