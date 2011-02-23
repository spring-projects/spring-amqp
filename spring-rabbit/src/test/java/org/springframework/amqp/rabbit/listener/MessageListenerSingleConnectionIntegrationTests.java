package org.springframework.amqp.rabbit.listener;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;

public class MessageListenerSingleConnectionIntegrationTests extends MessageListenerCachingConnectionIntegrationTests {

	protected ConnectionFactory createConnectionFactory() {
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory();
		connectionFactory.setPort(BrokerTestUtils.getPort());
		return connectionFactory;
	}

}
