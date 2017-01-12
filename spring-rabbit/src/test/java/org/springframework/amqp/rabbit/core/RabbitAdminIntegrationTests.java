/*
 * Copyright 2002-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.core;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.UUID;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.AbstractExchange;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.AutoRecoverConnectionNotCurrentlyOpenException;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.context.support.GenericApplicationContext;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Dave Syer
 * @author Ed Scriven
 * @author Gary Russell
 * @author Gunnar Hillert
 * @author Artem Bilan
 */
public class RabbitAdminIntegrationTests {

	private final CachingConnectionFactory connectionFactory = new CachingConnectionFactory();

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isBrokerAndManagementRunning();

	private GenericApplicationContext context;

	private RabbitAdmin rabbitAdmin;

	public RabbitAdminIntegrationTests() {
		connectionFactory.setPort(BrokerTestUtils.getPort());
	}

	@Before
	public void init() {
		connectionFactory.setHost("localhost");
		context = new GenericApplicationContext();
		context.refresh();
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
		if (connectionFactory != null) {
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
		connectionFactory1.setHost("localhost");
		connectionFactory1.setPort(BrokerTestUtils.getPort());
		CachingConnectionFactory connectionFactory2 = new CachingConnectionFactory();
		connectionFactory2.setHost("localhost");
		connectionFactory2.setPort(BrokerTestUtils.getPort());
		Queue queue = new Queue("test.queue", false, true, true);
		rabbitAdmin.deleteQueue(queue.getName());
		new RabbitAdmin(connectionFactory1).declareQueue(queue);
		try {
			new RabbitAdmin(connectionFactory2).declareQueue(queue);
		}
		finally {
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
		connectionFactory1.setHost("localhost");
		connectionFactory1.setPort(BrokerTestUtils.getPort());
		CachingConnectionFactory connectionFactory2 = new CachingConnectionFactory();
		connectionFactory2.setHost("localhost");
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
	public void testQueueWithoutName() throws Exception {

		final Queue queue = new Queue("", true, false, true);
		String generatedName = rabbitAdmin.declareQueue(queue);

		assertEquals("", queue.getName());
		Queue queueWithGeneratedName = new Queue(generatedName, true, false, true);
		assertTrue(queueExists(queueWithGeneratedName));

		// Stop and broker retains queue (only verifiable in native API)
		connectionFactory.destroy();
		assertTrue(queueExists(queueWithGeneratedName));

		// Start and queue still exists
		connectionFactory.createConnection();
		assertTrue(queueExists(queueWithGeneratedName));

		// Queue manually deleted
		assertTrue(rabbitAdmin.deleteQueue(generatedName));
		assertFalse(queueExists(queueWithGeneratedName));

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
	public void testDeleteExchangeWithInternalOption() throws Exception {
		String exchangeName = "test.exchange.internal";
		AbstractExchange exchange = new DirectExchange(exchangeName);
		exchange.setInternal(true);
		rabbitAdmin.declareExchange(exchange);

		Exchange exchange2 = getExchange(exchangeName);
		assertThat(exchange2, instanceOf(DirectExchange.class));
		assertTrue(exchange2.isInternal());

		boolean result = rabbitAdmin.deleteExchange(exchangeName);

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
		}
		catch (AmqpIOException ex) {
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
		}
		catch (AmqpIOException ex) {
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
	public void testQueueDeclareBad() {
		this.rabbitAdmin.setIgnoreDeclarationExceptions(true);
		Queue queue = new AnonymousQueue();
		assertEquals(queue.getName(), this.rabbitAdmin.declareQueue(queue));
		queue = new Queue(queue.getName());
		assertNull(this.rabbitAdmin.declareQueue(queue));
		this.rabbitAdmin.deleteQueue(queue.getName());
	}

	@Test
	public void testDeclareDelayedExchange() throws Exception {
		DirectExchange exchange = new DirectExchange("test.delayed.exchange");
		exchange.setDelayed(true);
		Queue queue = new Queue(UUID.randomUUID().toString(), true, false, false);
		String exchangeName = exchange.getName();
		Binding binding = new Binding(queue.getName(), DestinationType.QUEUE, exchangeName, queue.getName(), null);

		try {
			this.rabbitAdmin.declareExchange(exchange);
		}
		catch (AmqpIOException e) {
			if (RabbitUtils.isExchangeDeclarationFailure(e)
					&& e.getCause().getCause().getMessage().contains("exchange type 'x-delayed-message'")) {
				Assume.assumeTrue("Broker does not have the delayed message exchange plugin installed", false);
			}
			else {
				throw e;
			}
		}
		catch (AutoRecoverConnectionNotCurrentlyOpenException e) {
			Assume.assumeTrue("Broker does not have the delayed message exchange plugin installed", false);
		}
		this.rabbitAdmin.declareQueue(queue);
		this.rabbitAdmin.declareBinding(binding);

		RabbitTemplate template = new RabbitTemplate(this.connectionFactory);
		template.setReceiveTimeout(10000);
		template.convertAndSend(exchangeName, queue.getName(), "foo", message -> {
			message.getMessageProperties().setDelay(1000);
			return message;
		});
		MessageProperties properties = new MessageProperties();
		properties.setDelay(500);
		template.send(exchangeName, queue.getName(),
				MessageBuilder.withBody("foo".getBytes()).andProperties(properties).build());
		long t1 = System.currentTimeMillis();
		Message received = template.receive(queue.getName());
		assertNotNull(received);
		assertEquals(Integer.valueOf(500), received.getMessageProperties().getReceivedDelay());
		received = template.receive(queue.getName());
		assertNotNull(received);
		assertEquals(Integer.valueOf(1000), received.getMessageProperties().getReceivedDelay());
		assertThat(System.currentTimeMillis() - t1, greaterThan(950L));

		Exchange exchange2 = getExchange(exchangeName);
		assertNotNull(exchange2);
		assertThat(exchange2, instanceOf(DirectExchange.class));
		assertTrue(exchange2.isDelayed());

		this.rabbitAdmin.deleteQueue(queue.getName());
		this.rabbitAdmin.deleteExchange(exchangeName);
	}

	private Exchange getExchange(String exchangeName) throws InterruptedException {
		RabbitManagementTemplate rmt = new RabbitManagementTemplate();
		int n = 0;
		Exchange exchange = rmt.getExchange(exchangeName);
		while (n++ < 100 && exchange == null) {
			Thread.sleep(100);
			exchange = rmt.getExchange(exchangeName);
		}
		return exchange;
	}

	/**
	 * Verify that a queue exists using the native Rabbit API to bypass all the connection and
	 * channel caching and callbacks in Spring AMQP.
	 *
	 * @param queue The queue to verify
	 * @return True if the queue exists
	 */
	private boolean queueExists(final Queue queue) throws Exception {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setPort(BrokerTestUtils.getPort());
		Connection connection = connectionFactory.newConnection();
		Channel channel = connection.createChannel();
		try {
			DeclareOk result = channel.queueDeclarePassive(queue.getName());
			return result != null;
		}
		catch (IOException e) {
			return e.getCause().getMessage().contains("RESOURCE_LOCKED");
		}
		finally {
			connection.close();
		}
	}

}
