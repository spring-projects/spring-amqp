/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.amqp.rabbit.junit;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 2.0.2
 *
 */
@RabbitAvailable(queues = "rabbitAvailableTests.queue", management = true)
public class RabbitAvailableTests {

	@Test
	public void test(ConnectionFactory connectionFactory) throws Exception {
		Connection conn = connectionFactory.newConnection();
		Channel channel = conn.createChannel();
		DeclareOk declareOk = channel.queueDeclarePassive("rabbitAvailableTests.queue");
		assertThat(declareOk.getConsumerCount()).isEqualTo(0);
		channel.close();
		conn.close();
	}

}
