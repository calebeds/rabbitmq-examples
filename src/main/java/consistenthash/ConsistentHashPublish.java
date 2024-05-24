package consistenthash;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ConsistentHashPublish {
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();


        channel.confirmSelect();

        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        for(int i = 0; i < 100000;i ++) {
            channel.basicPublish("ex.hash", String.valueOf(i), builder.build(), "".getBytes("UTF-8"));
        }

        channel.waitForConfirms(10000);

        System.out.println("Done publishing");

        conn.close();
    }
}
