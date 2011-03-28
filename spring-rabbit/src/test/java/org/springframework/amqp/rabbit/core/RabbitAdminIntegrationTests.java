package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.context.support.GenericApplicationContext;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitAdminIntegrationTests {

	private CachingConnectionFactory connectionFactory = new CachingConnectionFactory();

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunning();

	private GenericApplicationContext context;

	private RabbitAdmin rabbitAdmin;

	public RabbitAdminIntegrationTests() {
		connectionFactory.setPort(BrokerTestUtils.getPort());
	}

	@Before
	public void init() {
		context = new GenericApplicationContext();
		rabbitAdmin = new RabbitAdmin(connectionFactory);
		rabbitAdmin.setApplicationContext(context);
		rabbitAdmin.setAutoStartup(true);
	}

	@After
	public void close() {
		if (context != null) {
			context.close();
		}
	}

	@Test
	public void testStartupWithBroker() throws Exception {
		Queue queue = new Queue("test.queue");
		context.getBeanFactory().registerSingleton("foo", queue);
		rabbitAdmin.deleteQueue(queue.getName());
		rabbitAdmin.afterPropertiesSet();
		assertTrue(rabbitAdmin.deleteQueue(queue.getName()));
	}

	@Test
	public void testStartupWithAutodelete() throws Exception {

		final Queue queue = new Queue("test.queue", false, true, true);
		context.getBeanFactory().registerSingleton("foo", queue);
		rabbitAdmin.deleteQueue(queue.getName());
		rabbitAdmin.afterPropertiesSet();
		
		final AtomicReference<Connection> connectionHolder = new AtomicReference<Connection>();

		RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
		// Force RabbitAdmin to initialize the queue
		boolean exists = rabbitTemplate.execute(new ChannelCallback<Boolean>() {
			public Boolean doInRabbit(Channel channel) throws Exception {
				DeclareOk result = channel.queueDeclarePassive(queue.getName());
				connectionHolder.set(channel.getConnection());
				return result != null;
			}
		});
		assertTrue("Expected Queue to exist", exists);

		assertTrue(queueExists(connectionHolder.get(), queue));
		connectionFactory.destroy();
		// Broker now deletes queue (only verifiable in native API)
		assertFalse(queueExists(null, queue));

		// Broker auto-deleted queue, but it is re-created by the connection listener
		exists = rabbitTemplate.execute(new ChannelCallback<Boolean>() {
			public Boolean doInRabbit(Channel channel) throws Exception {
				DeclareOk result = channel.queueDeclarePassive(queue.getName());
				connectionHolder.set(channel.getConnection());
				return result != null;
			}
		});
		assertTrue("Expected Queue to exist", exists);

		assertTrue(queueExists(connectionHolder.get(), queue));
		assertTrue(rabbitAdmin.deleteQueue(queue.getName()));
		assertFalse(queueExists(null, queue));

	}

	/**
	 * Use native Rabbit API to test queue, bypassing all the connection and channel caching and callbacks in Spring
	 * AMQP.
	 * 
	 * @param connection the raw connection to use
	 * @param queue the Queue to test
	 * @return true if the queue exists
	 */
	private boolean queueExists(Connection connection, Queue queue) throws Exception {
		if (connection==null) {
			ConnectionFactory connectionFactory = new ConnectionFactory();
			connectionFactory.setPort(BrokerTestUtils.getPort());
			connection = connectionFactory.newConnection();
		}
		Channel channel = connection.createChannel();
		try {
			DeclareOk result = channel.queueDeclarePassive(queue.getName());
			return result != null;
		} catch (Exception e) {
			return false;
		}
	}

}
