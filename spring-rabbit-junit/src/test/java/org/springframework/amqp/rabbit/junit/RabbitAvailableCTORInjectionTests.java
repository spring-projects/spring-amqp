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
 * @author Artem Bilan
 *
 * @since 2.0.2
 *
 */
@RabbitAvailable(queues = RabbitAvailableCTORInjectionTests.TEST_QUEUE)
public class RabbitAvailableCTORInjectionTests {

	static final String TEST_QUEUE = "rabbitAvailableCTORInjectionTests.queue";

	private final ConnectionFactory connectionFactory;

	public RabbitAvailableCTORInjectionTests(BrokerRunningSupport brokerRunning) {
		this.connectionFactory = brokerRunning.getConnectionFactory();
	}

	@Test
	public void test(ConnectionFactory cf) throws Exception {
		assertThat(this.connectionFactory).isSameAs(cf);
		Connection conn = this.connectionFactory.newConnection();
		Channel channel = conn.createChannel();
		DeclareOk declareOk = channel.queueDeclarePassive(TEST_QUEUE);
		assertThat(declareOk.getConsumerCount()).isEqualTo(0);
		channel.close();
		conn.close();
	}

}
