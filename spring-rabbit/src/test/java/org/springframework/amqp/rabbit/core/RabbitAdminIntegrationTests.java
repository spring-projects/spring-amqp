/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
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
		rabbitAdmin.deleteQueue("test.queue");
		// Force connection factory to forget that it has been used to delete the queue
		connectionFactory.destroy();
		rabbitAdmin.setApplicationContext(context);
		rabbitAdmin.setAutoStartup(true);
	}

	@After
	public void close() {
		if (context != null) {
			context.close();
		}
		if (connectionFactory!=null) {
			connectionFactory.destroy();
		}
	}

	@Test
	public void testStartupWithLazyDeclaration() throws Exception {
		Queue queue = new Queue("test.queue");
		context.getBeanFactory().registerSingleton("foo", queue);
		rabbitAdmin.afterPropertiesSet();
		// A new connection is initialized so the queue is declared
		assertTrue(rabbitAdmin.deleteQueue(queue.getName()));
	}

	@Test(expected = AmqpIOException.class)
	public void testDoubleDeclarationOfExclusiveQueue() throws Exception {
		// Expect exception because the queue is locked when it is declared a second time.
		CachingConnectionFactory connectionFactory1 = new CachingConnectionFactory();
		connectionFactory1.setPort(BrokerTestUtils.getPort());
		CachingConnectionFactory connectionFactory2 = new CachingConnectionFactory();
		connectionFactory2.setPort(BrokerTestUtils.getPort());
		Queue queue = new Queue("test.queue", false, true, true);
		rabbitAdmin.deleteQueue(queue.getName());
		new RabbitAdmin(connectionFactory1).declareQueue(queue);
		try {
			new RabbitAdmin(connectionFactory2).declareQueue(queue);
		} finally {
			// Need to release the connection so the exclusive queue is deleted
			connectionFactory1.destroy();
			connectionFactory2.destroy();
		}
	}

	@Test
	public void testDoubleDeclarationOfAutodeleteQueue() throws Exception {
		// No error expected here: the queue is autodeleted when the last consumer is cancelled, but this one never has
		// any consumers.
		CachingConnectionFactory connectionFactory1 = new CachingConnectionFactory();
		connectionFactory1.setPort(BrokerTestUtils.getPort());
		CachingConnectionFactory connectionFactory2 = new CachingConnectionFactory();
		connectionFactory2.setPort(BrokerTestUtils.getPort());
		Queue queue = new Queue("test.queue", false, false, true);
		new RabbitAdmin(connectionFactory1).declareQueue(queue);
		new RabbitAdmin(connectionFactory2).declareQueue(queue);
		connectionFactory1.destroy();
		connectionFactory2.destroy();
	}

	@Test
	public void testQueueWithAutoDelete() throws Exception {

		final Queue queue = new Queue("test.queue", false, true, true);
		context.getBeanFactory().registerSingleton("foo", queue);
		rabbitAdmin.afterPropertiesSet();

		// Queue created on spring startup
		rabbitAdmin.initialize();
		assertTrue(queueExists(queue));

		// Stop and broker deletes queue (only verifiable in native API)
		connectionFactory.destroy();
		assertFalse(queueExists(queue));

		// Start and queue re-created by the connection listener
		connectionFactory.createConnection();
		assertTrue(queueExists(queue));

		// Queue manually deleted
		assertTrue(rabbitAdmin.deleteQueue(queue.getName()));
		assertFalse(queueExists(queue));

	}

	@Test
	public void testQueueWithoutAutoDelete() throws Exception {

		final Queue queue = new Queue("test.queue", false, false, false);
		context.getBeanFactory().registerSingleton("foo", queue);
		rabbitAdmin.afterPropertiesSet();

		// Queue created on Spring startup
		rabbitAdmin.initialize();
		assertTrue(queueExists(queue));

		// Stop and broker retains queue (only verifiable in native API)
		connectionFactory.destroy();
		assertTrue(queueExists(queue));

		// Start and queue still exists
		connectionFactory.createConnection();
		assertTrue(queueExists(queue));

		// Queue manually deleted
		assertTrue(rabbitAdmin.deleteQueue(queue.getName()));
		assertFalse(queueExists(queue));

		connectionFactory.destroy();
	}

	@Test
	public void testDeclareExchangeWithDefaultExchange() throws Exception {
		Exchange exchange = new DirectExchange(RabbitAdmin.DEFAULT_EXCHANGE_NAME);

		rabbitAdmin.declareExchange(exchange);

		// Pass by virtue of RabbitMQ not firing a 403 reply code
	}

	@Test
	public void testSpringWithDefaultExchange() throws Exception {
		Exchange exchange = new DirectExchange(RabbitAdmin.DEFAULT_EXCHANGE_NAME);
		context.getBeanFactory().registerSingleton("foo", exchange);
		rabbitAdmin.afterPropertiesSet();

		rabbitAdmin.initialize();

		// Pass by virtue of RabbitMQ not firing a 403 reply code
	}

	@Test
	public void testDeleteExchangeWithDefaultExchange() throws Exception {
		boolean result = rabbitAdmin.deleteExchange(RabbitAdmin.DEFAULT_EXCHANGE_NAME);

		assertTrue(result);
	}

	@Test
	public void testDeclareBindingWithDefaultExchangeImplicitBinding() throws Exception {
		Exchange exchange = new DirectExchange(RabbitAdmin.DEFAULT_EXCHANGE_NAME);
		String queueName = "test.queue";
		final Queue queue = new Queue(queueName, false, false, false);
		rabbitAdmin.declareQueue(queue);
		Binding binding = new Binding(queueName, DestinationType.QUEUE, exchange.getName(), queueName, null);

		rabbitAdmin.declareBinding(binding);

		// Pass by virtue of RabbitMQ not firing a 403 reply code for both exchange and binding declaration
		assertTrue(queueExists(queue));
	}

	@Test
	public void testSpringWithDefaultExchangeImplicitBinding() throws Exception {
		Exchange exchange = new DirectExchange(RabbitAdmin.DEFAULT_EXCHANGE_NAME);
		context.getBeanFactory().registerSingleton("foo", exchange);
		String queueName = "test.queue";
		final Queue queue = new Queue(queueName, false, false, false);
		context.getBeanFactory().registerSingleton("bar", queue);
		Binding binding = new Binding(queueName, DestinationType.QUEUE, exchange.getName(), queueName, null);
		context.getBeanFactory().registerSingleton("baz", binding);
		rabbitAdmin.afterPropertiesSet();

		rabbitAdmin.initialize();

		// Pass by virtue of RabbitMQ not firing a 403 reply code for both exchange and binding declaration
		assertTrue(queueExists(queue));
	}

	@Test
	public void testRemoveBindingWithDefaultExchangeImplicitBinding() throws Exception {
		String queueName = "test.queue";
		final Queue queue = new Queue(queueName, false, false, false);
		rabbitAdmin.declareQueue(queue);
		Binding binding = new Binding(queueName, DestinationType.QUEUE, RabbitAdmin.DEFAULT_EXCHANGE_NAME, queueName, null);

		rabbitAdmin.removeBinding(binding);

		// Pass by virtue of RabbitMQ not firing a 403 reply code
	}

	@Test
	public void testDeclareBindingWithDefaultExchangeNonImplicitBinding() throws Exception {
		Exchange exchange = new DirectExchange(RabbitAdmin.DEFAULT_EXCHANGE_NAME);
		String queueName = "test.queue";
		final Queue queue = new Queue(queueName, false, false, false);
		rabbitAdmin.declareQueue(queue);
		Binding binding = new Binding(queueName, DestinationType.QUEUE, exchange.getName(), "test.routingKey", null);

		try {
			rabbitAdmin.declareBinding(binding);
		} catch (AmqpIOException ex) {
			Throwable cause = ex;
			Throwable rootCause = null;
			while (cause != null) {
				rootCause = cause;
				cause = cause.getCause();
			}
			assertTrue(rootCause.getMessage().contains("reply-code=403"));
			assertTrue(rootCause.getMessage().contains("operation not permitted on the default exchange"));
		}
	}

	@Test
	public void testSpringWithDefaultExchangeNonImplicitBinding() throws Exception {
		Exchange exchange = new DirectExchange(RabbitAdmin.DEFAULT_EXCHANGE_NAME);
		context.getBeanFactory().registerSingleton("foo", exchange);
		String queueName = "test.queue";
		final Queue queue = new Queue(queueName, false, false, false);
		context.getBeanFactory().registerSingleton("bar", queue);
		Binding binding = new Binding(queueName, DestinationType.QUEUE, exchange.getName(), "test.routingKey", null);
		context.getBeanFactory().registerSingleton("baz", binding);
		rabbitAdmin.afterPropertiesSet();

		try {
			rabbitAdmin.declareBinding(binding);
		} catch (AmqpIOException ex) {
			Throwable cause = ex;
			Throwable rootCause = null;
			while (cause != null) {
				rootCause = cause;
				cause = cause.getCause();
			}
			assertTrue(rootCause.getMessage().contains("reply-code=403"));
			assertTrue(rootCause.getMessage().contains("operation not permitted on the default exchange"));
		}
	}

	/**
	 * Verify that a queue exists using the native Rabbit API to bypass all the connection and
	 * channel caching and callbacks in Spring AMQP.
	 *
	 * @param Queue The queue to verify
	 * @return True if the queue exists
	 */
	private boolean queueExists(final Queue queue) throws Exception {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setPort(BrokerTestUtils.getPort());
		Connection connection = connectionFactory.newConnection();
		Channel channel = connection.createChannel();
		try {
			DeclareOk result = channel.queueDeclarePassive(queue.getName());
			return result != null;
		} catch (IOException e) {
			if (e.getCause().getMessage().contains("RESOURCE_LOCKED")) {
				return true;
			}
			return false;
		} finally {
			connection.close();
		}
	}
}
