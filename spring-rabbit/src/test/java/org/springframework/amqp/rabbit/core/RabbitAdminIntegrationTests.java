package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.context.support.GenericApplicationContext;

public class RabbitAdminIntegrationTests {

	private static Queue queue = new Queue("test.queue");

	private CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
	
	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunning();
	
	public RabbitAdminIntegrationTests() {
		connectionFactory.setPort(BrokerTestUtils.getPort());
	}
	
	@Test
	public void testStartupWithBroker() throws Exception {
		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.getBeanFactory().registerSingleton("foo", queue);
		RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
		rabbitAdmin.setApplicationContext(applicationContext);
		rabbitAdmin.setAutoStartup(true);
		rabbitAdmin.deleteQueue(queue.getName());
		rabbitAdmin.afterPropertiesSet();
		assertTrue(rabbitAdmin.deleteQueue(queue.getName()));
	}

}
