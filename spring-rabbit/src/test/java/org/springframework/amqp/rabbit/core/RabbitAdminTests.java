package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.fail;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.context.support.GenericApplicationContext;

public class RabbitAdminTests {
	
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testSettingOfNullRabbitTemplate() {
		ConnectionFactory connectionFactory = null;
		try {
			new RabbitAdmin(connectionFactory);
			fail("should have thrown IllegalStateException when RabbitTemplate is not set.");
		} catch (IllegalArgumentException e) {

		}
	}

	@Test
	public void testNoFailOnStartupWithMissingBroker() throws Exception {
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory("foo");
		connectionFactory.setPort(434343);
		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.getBeanFactory().registerSingleton("foo", new Queue("queue"));
		RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
		rabbitAdmin.setApplicationContext(applicationContext);
		rabbitAdmin.setAutoStartup(true);
		rabbitAdmin.afterPropertiesSet();
	}

	@Test
	public void testFailOnFirstUseWithMissingBroker() throws Exception {
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory("foo");
		connectionFactory.setPort(434343);
		GenericApplicationContext applicationContext = new GenericApplicationContext();
		applicationContext.getBeanFactory().registerSingleton("foo", new Queue("queue"));
		RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
		rabbitAdmin.setApplicationContext(applicationContext);
		rabbitAdmin.setAutoStartup(true);
		rabbitAdmin.afterPropertiesSet();
		exception.expect(IllegalArgumentException.class);
		rabbitAdmin.declareQueue();
	}

}
