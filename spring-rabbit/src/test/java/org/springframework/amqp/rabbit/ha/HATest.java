package org.springframework.amqp.rabbit.ha;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.impl.AMQConnection;
import com.rabbitmq.client.impl.SocketFrameHandler;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Unit test checking basic recovery functionality of HA Rabbit MQ client.
 */
public class HATest {

    @Test
    @Ignore
    public void breakRabbitMQConnection() throws IOException, InterruptedException {
        HAConnectionFactory connectionFactory = new HAConnectionFactory(new ScheduledThreadPoolExecutor(2), 100, 100l);
        connectionFactory.setHost("127.0.0.1");
        HAConnection connection = (HAConnection) connectionFactory.newConnection();
        HAChannel channel = connection.createChannel();

        AMQP.Exchange.DeclareOk declareOk = channel.exchangeDeclare("testExchange", "direct", false, true, new HashMap<String, Object>());

        //Break the underlying socket.
        AMQConnection underlyingConnection = (AMQConnection) connection.getUnderlyingConnection();
        SocketFrameHandler frameHandler = (SocketFrameHandler) underlyingConnection.getFrameHandler();
        frameHandler._socket.close();

        Channel underlyingChannel = channel.getUnderlyingChannel();

        Thread.sleep(1000l); //time to recover...

        Assert.assertNotSame(underlyingConnection, connection.getUnderlyingConnection());
        Assert.assertNotSame(underlyingChannel, channel.getUnderlyingChannel());

        channel.close();
        connection.close();
    }
}
