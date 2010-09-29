package org.springframework.amqp.rabbit.connection;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.test.BrokerRunning;

public class CachingConnectionFactoryIntegrationTests {

	private CachingConnectionFactory connectionFactory = new CachingConnectionFactory();

	@Rule
	public static BrokerRunning brokerIsRunning = BrokerRunning.isRunning();
	
	@Rule
	public ExpectedException exception = ExpectedException.none();
	
	@After
	public void close() {
		// Release resources
		connectionFactory.resetConnection();
	}

	@Test
	public void testSendAndReceiveFromVolatileQueue() throws Exception {

		RabbitTemplate template = new RabbitTemplate(connectionFactory);

		RabbitAdmin admin = new RabbitAdmin(template);
		Queue queue = admin.declareQueue();
		template.convertAndSend(queue.getName(), "message");
		String result = (String) template.receiveAndConvert(queue.getName());
		assertEquals("message", result);

	}

	@Test
	public void testSendAndReceiveFromVolatileQueueAfterImplicitRemoval() throws Exception {

		RabbitTemplate template = new RabbitTemplate(connectionFactory);

		RabbitAdmin admin = new RabbitAdmin(template);
		Queue queue = admin.declareQueue();
		template.convertAndSend(queue.getName(), "message");

		// Force a physical close of the channel
		connectionFactory.resetConnection();
		
		// The queue was removed when the channel was closed 
		exception.expect(AmqpIOException.class);

		String result = (String) template.receiveAndConvert(queue.getName());
		assertEquals("message", result);

	}

}
