package lazyqueue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

public class LazyQueueExample {
    private static final String QUEUE_NAME = "q.test-queue-1";
    private static final int MESSAGE_COUNT = 1_000_000;

    public static void produceMessages(int count) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setVirtualHost("/development"); //change this in case virtual host is different
        factory.setPort(5672);
        try(Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()
        ) {
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.queuePurge(QUEUE_NAME);

            ByteBuffer buffer = ByteBuffer.allocate(1000);

            Stream.iterate(0, n -> n + 1)
                    .limit(MESSAGE_COUNT)
                    .forEach(i -> {
                        try {
                            channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, buffer.array());
                        } catch (IOException e) {
                            System.out.println(e);
                        }
                    });
        } catch (TimeoutException | IOException e) {
            System.out.println(e);
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.nanoTime();
        produceMessages(MESSAGE_COUNT);
        long end = System.nanoTime();
        System.out.format("Published %,d messages in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
    }
}
