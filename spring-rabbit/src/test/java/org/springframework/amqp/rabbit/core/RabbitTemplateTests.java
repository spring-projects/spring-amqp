/*
 * Copyright 2002-2022 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.ReceiveAndReplyCallback;
import org.springframework.amqp.rabbit.connection.AbstractRoutingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.PublisherCallbackChannel;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.connection.SimpleRoutingConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.transaction.RabbitTransactionManager;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.SerializationUtils;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AuthenticationFailureException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQImpl;
import com.rabbitmq.client.impl.AMQImpl.Queue.DeclareOk;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Mohammad Hewedy
 * @since 1.0.1
 *
 */
public class RabbitTemplateTests {

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

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);

		given(mockChannel.isOpen()).willReturn(true);

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
		assertThat(message.getBody()).isSameAs(payload);
	}

	@Test
	public void testConvertString() throws Exception {
		RabbitTemplate template = new RabbitTemplate();
		String payload = "Hello, world!";
		Message message = template.convertMessageIfNecessary(payload);
		assertThat(new String(message.getBody(), SimpleMessageConverter.DEFAULT_CHARSET)).isEqualTo(payload);
	}

	@Test
	public void testConvertSerializable() {
		RabbitTemplate template = new RabbitTemplate();
		Long payload = 43L;
		Message message = template.convertMessageIfNecessary(payload);
		assertThat(SerializationUtils.deserialize(message.getBody())).isEqualTo(payload);
	}

	@Test
	public void testConvertMessage() {
		RabbitTemplate template = new RabbitTemplate();
		Message input = new Message("Hello, world!".getBytes(), new MessageProperties());
		Message message = template.convertMessageIfNecessary(input);
		assertThat(message).isSameAs(input);
	}

	@Test // AMQP-249
	public void dontHangConsumerThread() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);

		given(mockChannel.queueDeclare()).willReturn(new AMQImpl.Queue.DeclareOk("foo", 0, 0));

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		willAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			return null;
		}).given(mockChannel).basicConsume(anyString(), anyBoolean(), anyString(),
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
		willAnswer(invocation -> {
			count.incrementAndGet();
			throw new AuthenticationFailureException("foo");
		}).given(mockConnectionFactory).newConnection(any(ExecutorService.class), anyString());

		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		connectionFactory.setExecutor(mock(ExecutorService.class));
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setRetryTemplate(new RetryTemplate());
		try {
			template.convertAndSend("foo", "bar", "baz");
		}
		catch (AmqpAuthenticationException e) {
			assertThat(e.getMessage()).contains("foo");
		}
		assertThat(count.get()).isEqualTo(3);
	}

	@Test
	public void testEvaluateDirectReplyToWithConnectException() throws Exception {
		org.springframework.amqp.rabbit.connection.ConnectionFactory mockConnectionFactory =
				mock(org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
		willThrow(new AmqpConnectException(null)).given(mockConnectionFactory).createConnection();
		RabbitTemplate template = new RabbitTemplate(mockConnectionFactory);
		assertThatThrownBy(() -> template.convertSendAndReceive("foo")).isInstanceOf(AmqpConnectException.class);
		assertThat(TestUtils.getPropertyValue(template, "evaluatedFastReplyTo", Boolean.class)).isFalse();
	}

	@Test
	public void testEvaluateDirectReplyToWithIOException() throws Exception {
		org.springframework.amqp.rabbit.connection.ConnectionFactory mockConnectionFactory =
				mock(org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
		willThrow(new AmqpIOException(null)).given(mockConnectionFactory).createConnection();
		RabbitTemplate template = new RabbitTemplate(mockConnectionFactory);
		assertThatThrownBy(() -> template.convertSendAndReceive("foo")).isInstanceOf(AmqpIOException.class);
		assertThat(TestUtils.getPropertyValue(template, "evaluatedFastReplyTo", Boolean.class)).isFalse();
	}

	@Test
	public void testEvaluateDirectReplyToWithIOExceptionDeclareFailed() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		AMQP.Channel.Close mockMethod = mock(AMQP.Channel.Close.class);
		given(mockMethod.getReplyCode()).willReturn(AMQP.NOT_FOUND);
		given(mockMethod.getClassId()).willReturn(RabbitUtils.QUEUE_CLASS_ID_50);
		given(mockMethod.getMethodId()).willReturn(RabbitUtils.DECLARE_METHOD_ID_10);
		willThrow(new ShutdownSignalException(true, false, mockMethod, null)).given(mockChannel)
				.queueDeclarePassive(Address.AMQ_RABBITMQ_REPLY_TO);
		given(mockChannel.queueDeclare()).willReturn(new AMQImpl.Queue.DeclareOk("foo", 0, 0));
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		connectionFactory.setExecutor(mock(ExecutorService.class));
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setReplyTimeout(1);
		template.convertSendAndReceive("foo");
		assertThat(TestUtils.getPropertyValue(template, "evaluatedFastReplyTo", Boolean.class)).isTrue();
		assertThat(TestUtils.getPropertyValue(template, "usingFastReplyTo", Boolean.class)).isFalse();
	}

	@Test
	public void testEvaluateDirectReplyToOK() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);
		given(mockChannel.isOpen()).willReturn(true);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockChannel.queueDeclarePassive(Address.AMQ_RABBITMQ_REPLY_TO))
				.willReturn(new AMQImpl.Queue.DeclareOk(Address.AMQ_RABBITMQ_REPLY_TO, 0, 0));
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		connectionFactory.setExecutor(mock(ExecutorService.class));
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setReplyTimeout(1);
		template.convertSendAndReceive("foo");
		assertThat(TestUtils.getPropertyValue(template, "evaluatedFastReplyTo", Boolean.class)).isTrue();
		assertThat(TestUtils.getPropertyValue(template, "usingFastReplyTo", Boolean.class)).isTrue();
	}

	@Test
	public void testRecovery() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		final AtomicInteger count = new AtomicInteger();
		willAnswer(invocation -> {
			count.incrementAndGet();
			throw new AuthenticationFailureException("foo");
		}).given(mockConnectionFactory).newConnection(any(ExecutorService.class), anyString());

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
		assertThat(count.get()).isEqualTo(3);
		assertThat(recoverInvoked.get()).isTrue();
	}

	@Test
	public void testPublisherConfirmsReturnsSetup() {
		org.springframework.amqp.rabbit.connection.ConnectionFactory cf =
				mock(org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
		given(cf.isPublisherConfirms()).willReturn(true);
		given(cf.isPublisherReturns()).willReturn(true);
		org.springframework.amqp.rabbit.connection.Connection conn =
				mock(org.springframework.amqp.rabbit.connection.Connection.class);
		given(cf.createConnection()).willReturn(conn);
		PublisherCallbackChannel channel = mock(PublisherCallbackChannel.class);
		given(conn.createChannel(false)).willReturn(channel);
		RabbitTemplate template = new RabbitTemplate(cf);
		template.convertAndSend("foo");
		verify(channel).addListener(template);
	}

	@Test
	public void testNoListenerAllowed1() {
		RabbitTemplate template = new RabbitTemplate();
		assertThatIllegalStateException()
				.isThrownBy(() -> template.expectedQueueNames());
	}

	@Test
	public void testNoListenerAllowed2() {
		RabbitTemplate template = new RabbitTemplate();
		template.setReplyAddress(Address.AMQ_RABBITMQ_REPLY_TO);
		assertThatIllegalStateException()
				.isThrownBy(() -> template.expectedQueueNames());
	}

	public final static AtomicInteger LOOKUP_KEY_COUNT = new AtomicInteger();

	@Test
	@SuppressWarnings("unchecked")
	public void testRoutingConnectionFactory() throws Exception {
		org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory1 =
				mock(org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
		org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory2 =
				mock(org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
		org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory3 =
				mock(org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
		org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory4 =
				mock(org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
		Map<Object, org.springframework.amqp.rabbit.connection.ConnectionFactory> factories =
				new HashMap<Object, org.springframework.amqp.rabbit.connection.ConnectionFactory>(2);
		factories.put("foo", connectionFactory1);
		factories.put("bar", connectionFactory2);
		factories.put("baz", connectionFactory3);
		factories.put("qux", connectionFactory4);


		AbstractRoutingConnectionFactory connectionFactory = new SimpleRoutingConnectionFactory();
		connectionFactory.setTargetConnectionFactories(factories);

		final RabbitTemplate template = new RabbitTemplate(connectionFactory);
		Expression sendExpression = new SpelExpressionParser()
				.parseExpression("T(org.springframework.amqp.rabbit.core.RabbitTemplateTests)" +
						".LOOKUP_KEY_COUNT.getAndIncrement() % 2 == 0 ? 'foo' : 'bar'");
		template.setSendConnectionFactorySelectorExpression(sendExpression);
		Expression receiveExpression = new SpelExpressionParser()
				.parseExpression("T(org.springframework.amqp.rabbit.core.RabbitTemplateTests)" +
						".LOOKUP_KEY_COUNT.getAndIncrement() % 2 == 0 ? 'baz' : 'qux'");
		template.setReceiveConnectionFactorySelectorExpression(receiveExpression);

		for (int i = 0; i < 3; i++) {
			try {
				template.convertAndSend("foo", "bar", "baz");
			}
			catch (@SuppressWarnings("unused") Exception e) {
				//Ignore it. Doesn't matter for this test.
			}
			try {
				template.receive("foo");
			}
			catch (@SuppressWarnings("unused") Exception e) {
				//Ignore it. Doesn't matter for this test.
			}
			try {
				template.receiveAndReply("foo", mock(ReceiveAndReplyCallback.class));
			}
			catch (@SuppressWarnings("unused") Exception e) {
				//Ignore it. Doesn't matter for this test.
			}
		}

		Mockito.verify(connectionFactory1, times(2)).createConnection();
		Mockito.verify(connectionFactory2, times(1)).createConnection();
		Mockito.verify(connectionFactory3, times(3)).createConnection();
		Mockito.verify(connectionFactory4, times(3)).createConnection();
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
		willReturn(dok).given(channel1).queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), anyMap());
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
				templateChannel.set(((ChannelProxy) c).getTargetChannel());
				return true;
			});
		});
		verify(channel1).txSelect();
		verify(channel1).queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), anyMap());
		assertThat(templateChannel.get()).isEqualTo(channel1);
		verify(channel1).txCommit();
	}

	@Test
	public void testShutdownWhileWaitingForReply() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockChannel.queueDeclare()).willReturn(new AMQImpl.Queue.DeclareOk("foo", 0, 0));
		final AtomicReference<ShutdownListener> listener = new AtomicReference<>();
		final CountDownLatch shutdownLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			listener.set(invocation.getArgument(0));
			shutdownLatch.countDown();
			return null;
		}).given(mockChannel).addShutdownListener(any());
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		connectionFactory.setExecutor(mock(ExecutorService.class));
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setReplyTimeout(60_000);
		Message input = new Message("Hello, world!".getBytes(), new MessageProperties());
		ExecutorService exec = Executors.newSingleThreadExecutor();
		exec.execute(() -> {
			try {
				shutdownLatch.await(10, TimeUnit.SECONDS);
			}
			catch (@SuppressWarnings("unused") InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			listener.get().shutdownCompleted(new ShutdownSignalException(true, false, null, null));
		});
		try {
			template.doSendAndReceiveWithTemporary("foo", "bar", input, null);
			fail("Expected exception");
		}
		catch (AmqpException e) {
			assertThat(e.getCause()).isInstanceOf(ShutdownSignalException.class);
		}
		exec.shutdownNow();
	}

	@Test
	public void testAddAndRemoveBeforePublishPostProcessors() {
		DoNothingMPP mpp1 = new DoNothingMPP();
		DoNothingMPP mpp2 = new DoNothingMPP();
		DoNothingMPP mpp3 = new DoNothingMPP();

		RabbitTemplate rabbitTemplate = new RabbitTemplate();
		rabbitTemplate.addBeforePublishPostProcessors(mpp1, mpp2);
		rabbitTemplate.addBeforePublishPostProcessors(mpp3);
		boolean removed = rabbitTemplate.removeBeforePublishPostProcessor(mpp1);
		@SuppressWarnings("unchecked")
		Collection<Object> beforePublishPostProcessors =
				(Collection<Object>) ReflectionTestUtils.getField(rabbitTemplate, "beforePublishPostProcessors");

		assertThat(removed).isEqualTo(true);
		assertThat(beforePublishPostProcessors).containsExactly(mpp2, mpp3);
	}

	@Test
	public void testAddAndRemoveAfterReceivePostProcessors() {
		DoNothingMPP mpp1 = new DoNothingMPP();
		DoNothingMPP mpp2 = new DoNothingMPP();
		DoNothingMPP mpp3 = new DoNothingMPP();

		RabbitTemplate rabbitTemplate = new RabbitTemplate();
		rabbitTemplate.addAfterReceivePostProcessors(mpp1, mpp2);
		rabbitTemplate.addAfterReceivePostProcessors(mpp3);
		boolean removed = rabbitTemplate.removeAfterReceivePostProcessor(mpp1);
		@SuppressWarnings("unchecked")
		Collection<Object> afterReceivePostProcessors =
				(Collection<Object>) ReflectionTestUtils.getField(rabbitTemplate, "afterReceivePostProcessors");

		assertThat(removed).isEqualTo(true);
		assertThat(afterReceivePostProcessors).containsExactly(mpp2, mpp3);
	}

	@Test
	public void testPublisherConnWithInvoke() {
		org.springframework.amqp.rabbit.connection.ConnectionFactory cf = mock(
				org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
		org.springframework.amqp.rabbit.connection.ConnectionFactory pcf = mock(
				org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
		given(cf.getPublisherConnectionFactory()).willReturn(pcf);
		RabbitTemplate template = new RabbitTemplate(cf);
		template.setUsePublisherConnection(true);
		org.springframework.amqp.rabbit.connection.Connection conn = mock(
				org.springframework.amqp.rabbit.connection.Connection.class);
		Channel channel = mock(Channel.class);
		given(pcf.createConnection()).willReturn(conn);
		given(conn.isOpen()).willReturn(true);
		given(conn.createChannel(false)).willReturn(channel);
		template.invoke(t -> null);
		verify(pcf).createConnection();
		verify(conn).createChannel(false);
	}

	@Test
	public void testPublisherConnWithInvokeInTx() {
		org.springframework.amqp.rabbit.connection.ConnectionFactory cf = mock(
				org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
		org.springframework.amqp.rabbit.connection.ConnectionFactory pcf = mock(
				org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
		given(cf.getPublisherConnectionFactory()).willReturn(pcf);
		RabbitTemplate template = new RabbitTemplate(cf);
		template.setUsePublisherConnection(true);
		template.setChannelTransacted(true);
		org.springframework.amqp.rabbit.connection.Connection conn = mock(
				org.springframework.amqp.rabbit.connection.Connection.class);
		Channel channel = mock(Channel.class);
		given(pcf.createConnection()).willReturn(conn);
		given(conn.isOpen()).willReturn(true);
		given(conn.createChannel(true)).willReturn(channel);
		template.invoke(t -> null);
		verify(pcf).createConnection();
		verify(conn).createChannel(true);
	}

	@Test
	void resourcesClearedAfterTxFails() throws IOException, TimeoutException {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockChannel.txSelect()).willReturn(mock(AMQP.Tx.SelectOk.class));
		given(mockChannel.txCommit()).willThrow(IllegalStateException.class);
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		connectionFactory.setExecutor(mock(ExecutorService.class));
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setChannelTransacted(true);
		TransactionTemplate tt = new TransactionTemplate(new RabbitTransactionManager(connectionFactory));
		assertThatIllegalStateException()
				.isThrownBy(() ->
						tt.execute(status -> {
							template.convertAndSend("foo", "bar");
							return null;
						}));
		assertThat(TransactionSynchronizationManager.hasResource(connectionFactory)).isFalse();
		assertThatIllegalStateException()
				.isThrownBy(() -> (TransactionSynchronizationManager.getSynchronizations()).isEmpty())
				.withMessage("Transaction synchronization is not active");
	}

	@Test
	void resourcesClearedAfterTxFailsWithSync() throws IOException, TimeoutException {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		Channel mockChannel = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);
		given(mockConnection.createChannel()).willReturn(mockChannel);
		given(mockChannel.txSelect()).willReturn(mock(AMQP.Tx.SelectOk.class));
		given(mockChannel.txCommit()).willThrow(IllegalStateException.class);
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		connectionFactory.setExecutor(mock(ExecutorService.class));
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setChannelTransacted(true);
		TransactionTemplate tt = new TransactionTemplate(new TestTransactionManager());
		tt.execute(status -> {
			template.convertAndSend("foo", "bar");
			return null;
		});
		assertThat(TransactionSynchronizationManager.hasResource(connectionFactory)).isFalse();
		assertThatIllegalStateException()
				.isThrownBy(() -> (TransactionSynchronizationManager.getSynchronizations()).isEmpty())
				.withMessage("Transaction synchronization is not active");
	}

	@SuppressWarnings("serial")
	private class TestTransactionManager extends AbstractPlatformTransactionManager {

		TestTransactionManager() {
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

	private class DoNothingMPP implements MessagePostProcessor {

		@Override
		public Message postProcessMessage(Message message) throws AmqpException {
			return message;
		}

	}

}
