/*
 * Copyright 2002-2018 the original author or authors.
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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.ReceiveAndReplyCallback;
import org.springframework.amqp.rabbit.connection.AbstractRoutingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.PublisherCallbackChannelConnectionFactory;
import org.springframework.amqp.rabbit.connection.SimpleRoutingConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.support.PublisherCallbackChannel;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.SerializationUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AuthenticationFailureException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.impl.AMQImpl;
import com.rabbitmq.client.impl.AMQImpl.Queue.DeclareOk;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.0.1
 *
 */
public class RabbitTemplateTests {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void returnConnectionAfterCommit() throws Exception {
		@SuppressWarnings("serial")
		TransactionTemplate txTemplate = new TransactionTemplate(new AbstractPlatformTransactionManager() {

			@Override
			protected Object doGetTransaction() throws TransactionException {
				return new Object();
			}

			@Override
			protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
			}

			@Override
			protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
			}

			@Override
			protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
			}
		});
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);

		when(mockChannel.isOpen()).thenReturn(true);

		CachingConnectionFactory connectionFactory = new CachingConnectionFactory(mockConnectionFactory);
		connectionFactory.setExecutor(mock(ExecutorService.class));
		final RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setChannelTransacted(true);

		txTemplate.execute(status -> {
			template.convertAndSend("foo", "bar");
			return null;
		});
		txTemplate.execute(status -> {
			template.convertAndSend("baz", "qux");
			return null;
		});
		verify(mockConnectionFactory, Mockito.times(1)).newConnection(any(ExecutorService.class), anyString());
		// ensure we used the same channel
		verify(mockConnection, times(1)).createChannel();
	}

	@Test
	public void testConvertBytes() {
		RabbitTemplate template = new RabbitTemplate();
		byte[] payload = "Hello, world!".getBytes();
		Message message = template.convertMessageIfNecessary(payload);
		assertSame(payload, message.getBody());
	}

	@Test
	public void testConvertString() throws Exception {
		RabbitTemplate template = new RabbitTemplate();
		String payload = "Hello, world!";
		Message message = template.convertMessageIfNecessary(payload);
		assertEquals(payload, new String(message.getBody(), SimpleMessageConverter.DEFAULT_CHARSET));
	}

	@Test
	public void testConvertSerializable() throws Exception {
		RabbitTemplate template = new RabbitTemplate();
		Long payload = 43L;
		Message message = template.convertMessageIfNecessary(payload);
		assertEquals(payload, SerializationUtils.deserialize(message.getBody()));
	}

	@Test
	public void testConvertMessage() throws Exception {
		RabbitTemplate template = new RabbitTemplate();
		Message input = new Message("Hello, world!".getBytes(), new MessageProperties());
		Message message = template.convertMessageIfNecessary(input);
		assertSame(input, message);
	}

	@Test // AMQP-249
	public void dontHangConsumerThread() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);
		when(mockConnection.createChannel()).thenReturn(mockChannel);

		when(mockChannel.queueDeclare()).thenReturn(new AMQImpl.Queue.DeclareOk("foo", 0, 0));

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		doAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			return null;
		}).when(mockChannel).basicConsume(anyString(), anyBoolean(), anyString(),
				anyBoolean(), anyBoolean(), isNull(), any(Consumer.class));
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		connectionFactory.setExecutor(mock(ExecutorService.class));
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setReplyTimeout(1);
		Message input = new Message("Hello, world!".getBytes(), new MessageProperties());
		template.doSendAndReceiveWithTemporary("foo", "bar", input, null);
		Envelope envelope = new Envelope(1, false, "foo", "bar");
		// used to hang here because of the SynchronousQueue and doSendAndReceive() already exited
		consumer.get().handleDelivery("foo", envelope, new AMQP.BasicProperties(), new byte[0]);
	}

	@Test
	public void testRetry() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		final AtomicInteger count = new AtomicInteger();
		doAnswer(invocation -> {
			count.incrementAndGet();
			throw new AuthenticationFailureException("foo");
		}).when(mockConnectionFactory).newConnection(any(ExecutorService.class), anyString());

		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		connectionFactory.setExecutor(mock(ExecutorService.class));
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setRetryTemplate(new RetryTemplate());
		try {
			template.convertAndSend("foo", "bar", "baz");
		}
		catch (AmqpAuthenticationException e) {
			assertThat(e.getMessage(), containsString("foo"));
		}
		assertEquals(3, count.get());
	}

	@Test
	public void testRecovery() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		final AtomicInteger count = new AtomicInteger();
		doAnswer(invocation -> {
			count.incrementAndGet();
			throw new AuthenticationFailureException("foo");
		}).when(mockConnectionFactory).newConnection(any(ExecutorService.class), anyString());

		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		connectionFactory.setExecutor(mock(ExecutorService.class));
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setRetryTemplate(new RetryTemplate());

		final AtomicBoolean recoverInvoked = new AtomicBoolean();

		template.setRecoveryCallback(context -> {
			recoverInvoked.set(true);
			return null;
		});
		template.convertAndSend("foo", "bar", "baz");
		assertEquals(3, count.get());
		assertTrue(recoverInvoked.get());
	}

	@Test
	public void testPublisherConfirmsReturnsSetup() {
		org.springframework.amqp.rabbit.connection.ConnectionFactory cf =
				mock(org.springframework.amqp.rabbit.connection.ConnectionFactory.class,
						withSettings().extraInterfaces(PublisherCallbackChannelConnectionFactory.class));
		PublisherCallbackChannelConnectionFactory pcccf = (PublisherCallbackChannelConnectionFactory) cf;
		when(pcccf.isPublisherConfirms()).thenReturn(true);
		when(pcccf.isPublisherReturns()).thenReturn(true);
		org.springframework.amqp.rabbit.connection.Connection conn =
				mock(org.springframework.amqp.rabbit.connection.Connection.class);
		when(cf.createConnection()).thenReturn(conn);
		PublisherCallbackChannel channel = mock(PublisherCallbackChannel.class);
		when(conn.createChannel(false)).thenReturn(channel);
		RabbitTemplate template = new RabbitTemplate(cf);
		template.convertAndSend("foo");
		verify(channel).addListener(template);
	}

	@Test
	public void testNoListenerAllowed1() {
		RabbitTemplate template = new RabbitTemplate();
		this.exception.expect(IllegalStateException.class);
		template.expectedQueueNames();
	}

	@Test
	public void testNoListenerAllowed2() {
		RabbitTemplate template = new RabbitTemplate();
		template.setReplyAddress(Address.AMQ_RABBITMQ_REPLY_TO);
		this.exception.expect(IllegalStateException.class);
		template.expectedQueueNames();
	}

	public final static AtomicInteger LOOKUP_KEY_COUNT = new AtomicInteger();

	@Test
	@SuppressWarnings("unchecked")
	public void testRoutingConnectionFactory() throws Exception {
		org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory1 =
				Mockito.mock(org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
		org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory2 =
				Mockito.mock(org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
		Map<Object, org.springframework.amqp.rabbit.connection.ConnectionFactory> factories =
				new HashMap<Object, org.springframework.amqp.rabbit.connection.ConnectionFactory>(2);
		factories.put("foo", connectionFactory1);
		factories.put("bar", connectionFactory2);


		AbstractRoutingConnectionFactory connectionFactory = new SimpleRoutingConnectionFactory();
		connectionFactory.setTargetConnectionFactories(factories);

		final RabbitTemplate template = new RabbitTemplate(connectionFactory);
		Expression expression = new SpelExpressionParser()
				.parseExpression("T(org.springframework.amqp.rabbit.core.RabbitTemplateTests)" +
						".LOOKUP_KEY_COUNT.getAndIncrement() % 2 == 0 ? 'foo' : 'bar'");
		template.setSendConnectionFactorySelectorExpression(expression);
		template.setReceiveConnectionFactorySelectorExpression(expression);

		for (int i = 0; i < 3; i++) {
			try {
				template.convertAndSend("foo", "bar", "baz");
			}
			catch (Exception e) {
				//Ignore it. Doesn't matter for this test.
			}
			try {
				template.receive("foo");
			}
			catch (Exception e) {
				//Ignore it. Doesn't matter for this test.
			}
			try {
				template.receiveAndReply("foo", mock(ReceiveAndReplyCallback.class));
			}
			catch (Exception e) {
				//Ignore it. Doesn't matter for this test.
			}
		}

		Mockito.verify(connectionFactory1, Mockito.times(5)).createConnection();
		Mockito.verify(connectionFactory2, Mockito.times(4)).createConnection();
	}

	@Test
	public void testNestedTxBinding() throws Exception {
		ConnectionFactory cf = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel1 = mock(Channel.class, "channel1");
		given(channel1.isOpen()).willReturn(true);
		Channel channel2 = mock(Channel.class, "channel2");
		given(channel2.isOpen()).willReturn(true);
		willReturn(connection).given(cf).newConnection(any(ExecutorService.class), anyString());
		given(connection.isOpen()).willReturn(true);
		given(connection.createChannel()).willReturn(channel1, channel2);
		DeclareOk dok = new DeclareOk("foo", 0, 0);
		willReturn(dok).given(channel1).queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), isNull());
		CachingConnectionFactory ccf = new CachingConnectionFactory(cf);
		ccf.setExecutor(mock(ExecutorService.class));
		RabbitTemplate rabbitTemplate = new RabbitTemplate(ccf);
		rabbitTemplate.setChannelTransacted(true);
		RabbitAdmin admin = new RabbitAdmin(rabbitTemplate);
		ApplicationContext ac = mock(ApplicationContext.class);
		willReturn(Collections.singletonMap("foo", new Queue("foo"))).given(ac).getBeansOfType(Queue.class);
		admin.setApplicationContext(ac);
		admin.afterPropertiesSet();
		AtomicReference<Channel> templateChannel = new AtomicReference<>();
		new TransactionTemplate(new TestTransactionManager()).execute(s -> {
			return rabbitTemplate.execute(c -> {
				templateChannel.set(c);
				return true;
			});
		});
		verify(channel1).txSelect();
		verify(channel1).queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), isNull());
		assertThat(((ChannelProxy) templateChannel.get()).getTargetChannel(), equalTo(channel1));
		verify(channel1).txCommit();
	}

	@SuppressWarnings("serial")
	private class TestTransactionManager extends AbstractPlatformTransactionManager {

		TestTransactionManager() {
			super();
		}

		@Override
		protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
		}

		@Override
		protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
		}

		@Override
		protected Object doGetTransaction() throws TransactionException {
			return new Object();
		}

		@Override
		protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
		}

	}

}
