/*
 * Copyright 2002-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.core;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.AbstractExchange;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.AutoRecoverConnectionNotCurrentlyOpenException;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.context.support.GenericApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * @author Dave Syer
 * @author Ed Scriven
 * @author Gary Russell
 * @author Gunnar Hillert
 * @author Artem Bilan
 * @author Raylax Grey
 */
@RabbitAvailable(management = true)
public class RabbitAdminIntegrationTests extends NeedsManagementTests {

	private final CachingConnectionFactory connectionFactory = new CachingConnectionFactory();

	private GenericApplicationContext context;

	private RabbitAdmin rabbitAdmin;

	public RabbitAdminIntegrationTests() {
		connectionFactory.setPort(BrokerTestUtils.getPort());
	}

	@BeforeEach
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

	@AfterEach
	public void close() {
		if (context != null) {
			context.close();
		}
		if (connectionFactory != null) {
			connectionFactory.destroy();
		}
	}

	@Test
	public void testStartupWithLazyDeclaration() {
		Queue queue = new Queue("test.queue");
		context.getBeanFactory().registerSingleton("foo", queue);
		rabbitAdmin.afterPropertiesSet();
		// A new connection is initialized so the queue is declared
		assertThat(rabbitAdmin.deleteQueue(queue.getName())).isTrue();
	}

	@Test
	public void testDoubleDeclarationOfExclusiveQueue() {
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
			assertThatThrownBy((() -> new RabbitAdmin(connectionFactory2).declareQueue((Queue) queue.clone())))
				.isInstanceOf(AmqpIOException.class);
		}
		finally {
			// Need to release the connection so the exclusive queue is deleted
			connectionFactory1.destroy();
			connectionFactory2.destroy();
		}
	}

	@Test
	public void testDoubleDeclarationOfAutodeleteQueue() {
		// No error expected here: the queue is auto-deleted when the last consumer is cancelled, but this one never has
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
		assertThat(queueExists(queue)).isTrue();

		// Stop and broker deletes queue (only verifiable in native API)
		connectionFactory.destroy();
		assertThat(queueExists(queue)).isFalse();

		// Start and queue re-created by the connection listener
		connectionFactory.createConnection();
		assertThat(queueExists(queue)).isTrue();

		// Queue manually deleted
		assertThat(rabbitAdmin.deleteQueue(queue.getName())).isTrue();
		assertThat(queueExists(queue)).isFalse();

	}

	@Test
	public void testQueueWithoutAutoDelete() throws Exception {

		final Queue queue = new Queue("test.queue", false, false, false);
		context.getBeanFactory().registerSingleton("foo", queue);
		rabbitAdmin.afterPropertiesSet();

		// Queue created on Spring startup
		rabbitAdmin.initialize();
		assertThat(queueExists(queue)).isTrue();

		// Stop and broker retains queue (only verifiable in native API)
		connectionFactory.destroy();
		assertThat(queueExists(queue)).isTrue();

		// Start and queue still exists
		connectionFactory.createConnection();
		assertThat(queueExists(queue)).isTrue();

		// Queue manually deleted
		assertThat(rabbitAdmin.deleteQueue(queue.getName())).isTrue();
		assertThat(queueExists(queue)).isFalse();

		connectionFactory.destroy();
	}

	@Test
	public void testQueueWithoutName() throws Exception {

		final Queue queue = new Queue("", true, false, true);
		String generatedName = rabbitAdmin.declareQueue(queue);

		assertThat(queue.getName()).isEqualTo("");
		Queue queueWithGeneratedName = new Queue(generatedName, true, false, true);
		assertThat(queueExists(queueWithGeneratedName)).isTrue();

		// Stop and broker retains queue (only verifiable in native API)
		connectionFactory.destroy();
		assertThat(queueExists(queueWithGeneratedName)).isTrue();

		// Start and queue still exists
		connectionFactory.createConnection();
		assertThat(queueExists(queueWithGeneratedName)).isTrue();

		// Queue manually deleted
		assertThat(rabbitAdmin.deleteQueue(generatedName)).isTrue();
		assertThat(queueExists(queueWithGeneratedName)).isFalse();

		connectionFactory.destroy();
	}

	@Test
	public void testDeclareExchangeWithDefaultExchange() {
		Exchange exchange = new DirectExchange(RabbitAdmin.DEFAULT_EXCHANGE_NAME);

		rabbitAdmin.declareExchange(exchange);

		// Pass by virtue of RabbitMQ not firing a 403 reply code
	}

	@Test
	public void testSpringWithDefaultExchange() {
		Exchange exchange = new DirectExchange(RabbitAdmin.DEFAULT_EXCHANGE_NAME);
		context.getBeanFactory().registerSingleton("foo", exchange);
		rabbitAdmin.afterPropertiesSet();

		assertThatNoException().isThrownBy(rabbitAdmin::initialize);
	}

	@Test
	public void testDeleteExchangeWithDefaultExchange() {
		boolean result = rabbitAdmin.deleteExchange(RabbitAdmin.DEFAULT_EXCHANGE_NAME);

		assertThat(result).isTrue();
	}

	@Test
	public void testDeleteExchangeWithInternalOption() throws Exception {
		String exchangeName = "test.exchange.internal";
		AbstractExchange exchange = new DirectExchange(exchangeName);
		exchange.setInternal(true);
		rabbitAdmin.declareExchange(exchange);

		Map<String, Object> exchange2 = getExchange(exchangeName);
		assertThat(exchange2.get("type")).isEqualTo(ExchangeTypes.DIRECT);
		assertThat(exchange2.get("internal")).isEqualTo(Boolean.TRUE);

		boolean result = rabbitAdmin.deleteExchange(exchangeName);

		assertThat(result).isTrue();
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
		assertThat(queueExists(queue)).isTrue();
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
		assertThat(queueExists(queue)).isTrue();
	}

	@Test
	public void testRemoveBindingWithDefaultExchangeImplicitBinding() {
		String queueName = "test.queue";
		final Queue queue = new Queue(queueName, false, false, false);
		rabbitAdmin.declareQueue(queue);
		Binding binding = new Binding(queueName, DestinationType.QUEUE, RabbitAdmin.DEFAULT_EXCHANGE_NAME, queueName, null);

		rabbitAdmin.removeBinding(binding);

		// Pass by virtue of RabbitMQ not firing a 403 reply code
	}

	@Test
	public void testDeclareBindingWithDefaultExchangeNonImplicitBinding() {
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
			assertThat(rootCause.getMessage().contains("reply-code=403")).isTrue();
			assertThat(rootCause.getMessage().contains("operation not permitted on the default exchange")).isTrue();
		}
	}

	@Test
	public void testSpringWithDefaultExchangeNonImplicitBinding() {
		Exchange exchange = new DirectExchange(RabbitAdmin.DEFAULT_EXCHANGE_NAME);
		context.getBeanFactory().registerSingleton("foo", exchange);
		String queueName = "test.queue";
		final Queue queue = new Queue(queueName, false, false, false);
		context.getBeanFactory().registerSingleton("bar", queue);
		Binding binding = new Binding(queueName, DestinationType.QUEUE, exchange.getName(), "test.routingKey", null);
		context.getBeanFactory().registerSingleton("baz", binding);
		this.rabbitAdmin.setRetryTemplate(null);
		this.rabbitAdmin.afterPropertiesSet();

		try {
			this.rabbitAdmin.declareBinding(binding);
		}
		catch (AmqpIOException ex) {
			Throwable cause = ex;
			Throwable rootCause = null;
			while (cause != null) {
				rootCause = cause;
				cause = cause.getCause();
			}
			assertThat(rootCause.getMessage().contains("reply-code=403")).isTrue();
			assertThat(rootCause.getMessage().contains("operation not permitted on the default exchange")).isTrue();
		}
	}

	@Test
	public void testQueueDeclareBad() {
		this.rabbitAdmin.setIgnoreDeclarationExceptions(true);
		Queue queue = new AnonymousQueue();
		assertThat(this.rabbitAdmin.declareQueue(queue)).isEqualTo(queue.getName());
		queue = new Queue(queue.getName());
		assertThat(this.rabbitAdmin.declareQueue(queue)).isNull();
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
				return;
			}
			else {
				throw e;
			}
		}
		catch (@SuppressWarnings("unused") AutoRecoverConnectionNotCurrentlyOpenException e) {
			return;
		}
		this.rabbitAdmin.declareQueue(queue);
		this.rabbitAdmin.declareBinding(binding);

		RabbitTemplate template = new RabbitTemplate(this.connectionFactory);
		template.setReceiveTimeout(10000);
		template.convertAndSend(exchangeName, queue.getName(), "foo", message -> {
			message.getMessageProperties().setDelayLong(1000L);
			return message;
		});
		MessageProperties properties = new MessageProperties();
		properties.setDelayLong(500L);
		template.send(exchangeName, queue.getName(),
				MessageBuilder.withBody("foo".getBytes()).andProperties(properties).build());
		long t1 = System.currentTimeMillis();
		Message received = template.receive(queue.getName());
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getDelayLong()).isEqualTo(500L);
		received = template.receive(queue.getName());
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getDelayLong()).isEqualTo(1000L);
		assertThat(System.currentTimeMillis() - t1).isGreaterThan(950L);

		Map<String, Object> exchange2 = getExchange(exchangeName);
		assertThat(arguments(exchange2).get("x-delayed-type")).isEqualTo(ExchangeTypes.DIRECT);
		assertThat(exchange2.get("type")).isEqualTo("x-delayed-message");

		this.rabbitAdmin.deleteQueue(queue.getName());
		this.rabbitAdmin.deleteExchange(exchangeName);
	}

	private Map<String, Object> getExchange(String exchangeName) {
		return await().pollDelay(Duration.ZERO)
				.until(() -> exchangeInfo(exchangeName), Objects::nonNull);
	}

	/**
	 * Verify that a queue exists using the native Rabbit API to bypass all the connection and
	 * channel caching and callbacks in Spring AMQP.
	 *
	 * @param queue The queue to verify
	 * @return True if the queue exists
	 */
	private boolean queueExists(final Queue queue) throws Exception {
		ConnectionFactory cf = new ConnectionFactory();
		cf.setHost("localhost");
		cf.setPort(BrokerTestUtils.getPort());
		try (Connection connection = cf.newConnection()) {
			Channel channel = connection.createChannel();
			DeclareOk result = channel.queueDeclarePassive(queue.getName());
			return result != null;
		}
		catch (IOException e) {
			return e.getCause().getMessage().contains("RESOURCE_LOCKED");
		}
	}

}
