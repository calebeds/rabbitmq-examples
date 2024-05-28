package basicconsume;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class BasicConsumeExample {
    static class SampleConsumer extends Thread {
        private final String queueName;


        SampleConsumer(String queueName) {
            this.queueName = queueName;
        }

        @Override
        public void run() {
            try {
                System.out.println("--> Running consumer");

                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                factory.setPort(5672);

                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();

                channel.queueDeclare(queueName, true, false, false, null);
                System.out.println(" [*] Waiting for messages...");

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

                    System.out.format("%s [x] Received ", message);
                    channel.basicReject(delivery.getEnvelope().getDeliveryTag(), true);

                };

                channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {});

                sleep(Long.MAX_VALUE);

            } catch (InterruptedException | IOException | TimeoutException e) {
                System.out.println(e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        SampleConsumer consumer = new SampleConsumer("q.test");

        consumer.start();

        consumer.join();

        System.out.println("Done");
    }
}
