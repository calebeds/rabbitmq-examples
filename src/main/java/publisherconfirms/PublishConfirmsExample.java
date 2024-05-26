package publisherconfirms;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

public class PublishConfirmsExample {
    private static final String QUEUE_NAME = "q.pub_confirms";
    private static final int MESSAGE_COUNT = 50_000;

    static Connection createConnection() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setPort(5672);
        return factory.newConnection();
    }

    private static final ReturnListener guaranteedDeliveryReturnListener = new ReturnListener() {
        @Override
        public void handleReturn(int i, String s, String s1, String s2, AMQP.BasicProperties basicProperties, byte[] bytes) throws IOException {

        }
    };

    private static final ConfirmListener guaranteedDeliveryConfirmListener = new ConfirmListener() {
        @Override
        public void handleAck(long l, boolean b) throws IOException {

        }

        @Override
        public void handleNack(long l, boolean b) throws IOException {

        }
    };

    static void publishMessagesIndividually(boolean isPubConfEnabled) throws Exception {
        try(Connection connection = createConnection()) {
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, true, null);
            if(isPubConfEnabled) {
                channel.confirmSelect();
            }
            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String message = String.valueOf(i);
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                if(isPubConfEnabled) {
                    channel.waitForConfirmsOrDie(5_000);
                }
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        } catch (TimeoutException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    static void publishMessagesInBatch() throws Exception {
        try(Connection connection = createConnection()) {
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, true, null);
            channel.confirmSelect();

            int batchSize = 100;
            int outstandingMessageCount = 0;

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                channel.basicPublish("", QUEUE_NAME, null, body.getBytes());
                outstandingMessageCount++;

                if(outstandingMessageCount == batchSize) {
                    channel.waitForConfirmsOrDie(5_000);
                    outstandingMessageCount = 0;
                }
            }
            if(outstandingMessageCount > 0) {
                channel.waitForConfirmsOrDie(5_000);
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages in batch in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    static boolean waitUntil(Duration timout, BooleanSupplier condition) throws InterruptedException {
        int waited = 0;
        while (!condition.getAsBoolean() && waited < timout.toMillis()) {
            Thread.sleep(100L);
            waited += 100;
        }
        return condition.getAsBoolean();
    }

    static void handlePublishConfirmsAsynchronously() throws Exception {
        try(Connection connection = createConnection()) {
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, true, null);
            channel.confirmSelect();

            ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();
            ConfirmCallback cleanOutstandingConfirms = (sequenceNumber, multiple) -> {
              if(multiple) {
                  ConcurrentNavigableMap<Long, String> confirmed =
                          outstandingConfirms.headMap(sequenceNumber, true);
                  confirmed.clear();
              } else {
                  outstandingConfirms.remove(sequenceNumber);
              }
            };

            channel.addConfirmListener(cleanOutstandingConfirms, (sequenceNumber, multiple) -> {
                String body = outstandingConfirms.get(sequenceNumber);
                System.err.format("Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n",
                        body, sequenceNumber, multiple);
            });

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                outstandingConfirms.put(channel.getNextPublishSeqNo(), body);
                channel.basicPublish("", QUEUE_NAME, null, body.getBytes());
            }

            if(!waitUntil(Duration.ofSeconds(60), outstandingConfirms::isEmpty)) {
                throw new IllegalStateException("All messages could not be confirmed in 60 seconds");
            }

            long end = System.nanoTime();
            System.out.format("Published %,d messages and handled confirms asynchronously in %,d ms%n",
                    MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    public static void main(String[] args) throws Exception {
        publishMessagesIndividually(false);
        publishMessagesIndividually(true);
        publishMessagesInBatch();
        handlePublishConfirmsAsynchronously();

        System.out.println("done");
    }
}
