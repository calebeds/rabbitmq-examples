package priorityqueue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

public class PriorityQueueExample {
    private static final String QUEUE_NAME = "q.test-priority";
    private static final int MESSAGE_COUNT = 300_000;
    private static final int MAX_PRIORITY = 255;

    public static void produceMessages(int count) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setVirtualHost("/development");//delete this line in case there is only the default host

        try(Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {
            Map<String, Object> args = new HashMap<>();
            args.put("x-max-priority", MAX_PRIORITY);

            channel.queueDeclare(QUEUE_NAME, true, false, false, args);
            channel.queuePurge(QUEUE_NAME);

            ByteBuffer buffer = ByteBuffer.allocate(1000);

            for (int i = 0; i < count; i++) {
                try {
                    AMQP.BasicProperties properties = new AMQP.BasicProperties
                            .Builder()
//                            .priority(1)
                            .priority(i%MAX_PRIORITY)
                            .deliveryMode(2)
                            .build();
                    channel.basicPublish("", QUEUE_NAME, properties, buffer.array());
                } catch (IOException e) {
                    System.out.println(e);
                }
            }
        } catch (TimeoutException | IOException e) {
            System.out.println(e);
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.nanoTime();
        produceMessages(MESSAGE_COUNT);
        long end = System.nanoTime();
        System.out.format("Published %,d messages in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end -start).toMillis());
    }
}
