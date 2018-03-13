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

package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.transaction.RabbitTransactionManager;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.NoRollbackRuleAttribute;
import org.springframework.transaction.interceptor.RollbackRuleAttribute;
import org.springframework.transaction.interceptor.RuleBasedTransactionAttribute;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;

/**
 * @author Gary Russell
 * @since 1.1.2
 *
 */
public abstract class ExternalTxManagerTests {

	/**
	 * Verifies that an up-stack RabbitTemplate uses the listener's
	 * channel (MessageListener).
	 */
	@Test
	public void testMessageListener() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		final Channel onlyChannel = mock(Channel.class);
		given(onlyChannel.isOpen()).willReturn(true);

		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(mockConnectionFactory);
		cachingConnectionFactory.setExecutor(mock(ExecutorService.class));

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();

		willAnswer(ensureOneChannelAnswer(onlyChannel, tooManyChannels)).given(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final CountDownLatch consumerLatch = new CountDownLatch(1);

		willAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumerLatch.countDown();
			return "consumerTag";
		}).given(onlyChannel)
				.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
						any(Consumer.class));

		final AtomicReference<CountDownLatch> commitLatch = new AtomicReference<>(new CountDownLatch(1));
		willAnswer(invocation -> {
			commitLatch.get().countDown();
			return null;
		}).given(onlyChannel).txCommit();
		final AtomicReference<CountDownLatch> rollbackLatch = new AtomicReference<>(new CountDownLatch(1));
		willAnswer(invocation -> {
			rollbackLatch.get().countDown();
			return null;
		}).given(onlyChannel).txRollback();
		willAnswer(invocation -> {
			return null;
		}).given(onlyChannel).basicAck(anyLong(), anyBoolean());

		final CountDownLatch latch = new CountDownLatch(1);
		AbstractMessageListenerContainer container = createContainer(cachingConnectionFactory);
		container.setMessageListener((MessageListener) message -> {
			RabbitTemplate rabbitTemplate = new RabbitTemplate(cachingConnectionFactory);
			rabbitTemplate.setChannelTransacted(true);
			// should use same channel as container
			rabbitTemplate.convertAndSend("foo", "bar", "baz");
			latch.countDown();
		});
		container.setQueueNames("queue");
		container.setChannelTransacted(true);
		container.setShutdownTimeout(100);
		DummyTxManager transactionManager = new DummyTxManager();
		container.setTransactionManager(transactionManager);
		RuleBasedTransactionAttribute transactionAttribute = new ListenerFailedRuleBasedTransactionAttribute();
		List<RollbackRuleAttribute> rollbackRules =
				Collections.singletonList(new NoRollbackRuleAttribute(IllegalStateException.class));
		transactionAttribute.setRollbackRules(rollbackRules);
		container.setTransactionAttribute(transactionAttribute);
		container.afterPropertiesSet();
		container.start();
		assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(),
				new byte[] { 0 });

		assertTrue(latch.await(10, TimeUnit.SECONDS));

		Exception e = tooManyChannels.get();
		if (e != null) {
			throw e;
		}

		verify(mockConnection, times(1)).createChannel();
		assertTrue(commitLatch.get().await(10, TimeUnit.SECONDS));
		verify(onlyChannel).basicAck(anyLong(), anyBoolean());
		verify(onlyChannel).txCommit();
		verify(onlyChannel).basicPublish(anyString(), anyString(), anyBoolean(),
				any(BasicProperties.class), any(byte[].class));

		// verify close() was never called on the channel
		DirectFieldAccessor dfa = new DirectFieldAccessor(cachingConnectionFactory);
		List<?> channels = (List<?>) dfa.getPropertyValue("cachedChannelsTransactional");
		assertEquals(0, channels.size());

		assertTrue(transactionManager.committed);
		transactionManager.committed = false;
		transactionManager.latch = new CountDownLatch(1);
		container.setMessageListener(m -> {
			throw new RuntimeException();
		});
		commitLatch.set(new CountDownLatch(1));
		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(),
				new byte[] { 0 });
		assertTrue(transactionManager.latch.await(10, TimeUnit.SECONDS));
		assertTrue(commitLatch.get().await(10, TimeUnit.SECONDS));
		assertTrue(transactionManager.rolledBack);
		assertTrue(rollbackLatch.get().await(10, TimeUnit.SECONDS));
		verify(onlyChannel).basicReject(anyLong(), anyBoolean());
		verify(onlyChannel, times(1)).txRollback();

		transactionManager.rolledBack = false;
		transactionManager.latch = new CountDownLatch(1);
		container.setMessageListener(m -> {
			throw new IllegalStateException();
		});
		commitLatch.set(new CountDownLatch(1));
		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(),
				new byte[] { 0 });
		assertTrue(transactionManager.latch.await(10, TimeUnit.SECONDS));
		assertTrue(commitLatch.get().await(10, TimeUnit.SECONDS));
		assertTrue(transactionManager.committed);
		verify(onlyChannel, times(2)).basicAck(anyLong(), anyBoolean());
		verify(onlyChannel, times(3)).txCommit(); // previous + reject commit for above + this one

		transactionManager.committed = false;
		transactionManager.latch = new CountDownLatch(1);
		container.setMessageListener(m -> {
			throw new AmqpRejectAndDontRequeueException("foo", new ImmediateAcknowledgeAmqpException("bar"));
		});
		commitLatch.set(new CountDownLatch(1));
		rollbackLatch.set(new CountDownLatch(1));
		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(),
				new byte[] { 0 });
		assertTrue(transactionManager.latch.await(10, TimeUnit.SECONDS));
		assertTrue(transactionManager.rolledBack);
		assertTrue(rollbackLatch.get().await(10, TimeUnit.SECONDS));
		assertTrue(commitLatch.get().await(10, TimeUnit.SECONDS));
		verify(onlyChannel, times(2)).basicReject(anyLong(), anyBoolean());
		verify(onlyChannel, times(2)).txRollback();

		transactionManager.rolledBack = false;
		transactionManager.latch = new CountDownLatch(1);
		container.setAfterReceivePostProcessors(m -> null);
		container.setMessageListener(m -> {
			// NOSONAR
		});
		commitLatch.set(new CountDownLatch(1));
		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(),
				new byte[] { 0 });
		assertTrue(transactionManager.latch.await(10, TimeUnit.SECONDS));
		assertTrue(commitLatch.get().await(10, TimeUnit.SECONDS));
		assertTrue(transactionManager.committed);
		verify(onlyChannel, times(3)).basicAck(anyLong(), anyBoolean());
		verify(onlyChannel, times(5)).txCommit();

		container.stop();
	}

	@Test
	public void testMessageListenerRollback() throws Exception {
		testMessageListenerRollbackGuts(true, TransactionDefinition.PROPAGATION_REQUIRED);
	}

	@Test
	public void testMessageListenerRollbackDontRequeue() throws Exception {
		testMessageListenerRollbackGuts(false, TransactionDefinition.PROPAGATION_REQUIRED);
	}

	@Test
	public void testMessageListenerRollbackNoBoundTransaction() throws Exception {
		testMessageListenerRollbackGuts(true, TransactionDefinition.PROPAGATION_NEVER);
	}

	@Test
	public void testMessageListenerRollbackDontRequeueNoBoundTransaction() throws Exception {
		testMessageListenerRollbackGuts(false, TransactionDefinition.PROPAGATION_NEVER);
	}

	/**
	 * Verifies that the channel is rolled back after an exception.
	 */
	private void testMessageListenerRollbackGuts(boolean expectRequeue, int propagation) throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		final Channel channel = mock(Channel.class);
		given(channel.isOpen()).willReturn(true);

		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(mockConnectionFactory);
		cachingConnectionFactory.setExecutor(mock(ExecutorService.class));

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();
		willAnswer(ensureOneChannelAnswer(channel, tooManyChannels)).given(mockConnection).createChannel();

		willAnswer(invocation -> channel).given(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final CountDownLatch consumerLatch = new CountDownLatch(1);

		willAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumerLatch.countDown();
			return "consumerTag";
		}).given(channel)
				.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
						any(Consumer.class));

		final CountDownLatch rollbackLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			rollbackLatch.countDown();
			return null;
		}).given(channel).txRollback();

		final CountDownLatch rejectLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			rejectLatch.countDown();
			return null;
		}).given(channel).basicReject(anyLong(), anyBoolean());
		willAnswer(invocation -> {
			rejectLatch.countDown();
			return null;
		}).given(channel).basicNack(anyLong(), anyBoolean(), anyBoolean());


		final CountDownLatch latch = new CountDownLatch(1);
		AbstractMessageListenerContainer container = createContainer(cachingConnectionFactory);
		container.setTransactionAttribute(new DefaultTransactionAttribute(propagation));
		container.setMessageListener(message -> {
			latch.countDown();
			throw expectRequeue
					? new RuntimeException("force rollback")
					: new AmqpRejectAndDontRequeueException("force rollback");
		});
		container.setQueueNames("queue");
		container.setChannelTransacted(true);
		container.setShutdownTimeout(100);
		container.setTransactionManager(new DummyTxManager());
		container.afterPropertiesSet();
		container.start();
		assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(),
				new byte[] { 0 });

		assertTrue(latch.await(10, TimeUnit.SECONDS));

		Exception e = tooManyChannels.get();
		if (e != null) {
			throw e;
		}

		verify(mockConnection, times(1)).createChannel();
		assertTrue(rejectLatch.await(10, TimeUnit.SECONDS));

		assertTrue(rollbackLatch.await(10, TimeUnit.SECONDS));
		if (propagation != TransactionDefinition.PROPAGATION_NEVER) {
			verify(channel).basicReject(anyLong(), eq(expectRequeue));
		}
		else {
			verify(channel).basicNack(anyLong(), eq(Boolean.TRUE), eq(expectRequeue));
		}
		container.stop();
	}

	@Test
	public void testMessageListenerCommit() throws Exception {
		testMessageListenerCommitGuts(TransactionDefinition.PROPAGATION_REQUIRED);
	}

	@Test
	public void testMessageListenerCommitNoBoundTransaction() throws Exception {
		testMessageListenerCommitGuts(TransactionDefinition.PROPAGATION_NEVER);
	}

	/**
	 * Verifies that the channel is committed.
	 */
	private void testMessageListenerCommitGuts(int propagation) throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		final Channel channel = mock(Channel.class);
		given(channel.isOpen()).willReturn(true);

		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(mockConnectionFactory);
		cachingConnectionFactory.setExecutor(mock(ExecutorService.class));

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();
		willAnswer(ensureOneChannelAnswer(channel, tooManyChannels)).given(mockConnection).createChannel();

		willAnswer(invocation -> channel).given(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final CountDownLatch consumerLatch = new CountDownLatch(1);

		willAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumerLatch.countDown();
			return "consumerTag";
		}).given(channel)
				.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
						any(Consumer.class));

		final CountDownLatch commitLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			commitLatch.countDown();
			return null;
		}).given(channel).txCommit();
		final CountDownLatch ackLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			ackLatch.countDown();
			return null;
		}).given(channel).basicAck(anyLong(), anyBoolean());

		final CountDownLatch latch = new CountDownLatch(1);
		AbstractMessageListenerContainer container = createContainer(cachingConnectionFactory);
		container.setTransactionAttribute(new DefaultTransactionAttribute(propagation));
		container.setMessageListener(message -> {
			latch.countDown();
		});
		container.setQueueNames("queue");
		container.setChannelTransacted(true);
		container.setShutdownTimeout(100);
		container.setTransactionManager(new DummyTxManager());
		container.afterPropertiesSet();
		container.start();
		assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(),
				new byte[] { 0 });

		assertTrue(latch.await(10, TimeUnit.SECONDS));

		Exception e = tooManyChannels.get();
		if (e != null) {
			throw e;
		}

		verify(mockConnection, times(1)).createChannel();

		assertTrue(ackLatch.await(10, TimeUnit.SECONDS));
		assertTrue(commitLatch.await(10, TimeUnit.SECONDS));
		verify(channel).basicAck(anyLong(), anyBoolean());
		container.stop();
	}

	/**
	 * Verifies that an up-stack RabbitTemplate does not use the listener's
	 * channel when it has its own connection factory.
	 */
	@Test
	public void testMessageListenerTemplateUsesDifferentConnectionFactory() throws Exception {
		ConnectionFactory listenerConnectionFactory = mock(ConnectionFactory.class);
		ConnectionFactory templateConnectionFactory = mock(ConnectionFactory.class);
		Connection listenerConnection = mock(Connection.class);
		Connection templateConnection = mock(Connection.class);
		final Channel listenerChannel = mock(Channel.class);
		Channel templateChannel = mock(Channel.class);
		given(listenerChannel.isOpen()).willReturn(true);
		given(templateChannel.isOpen()).willReturn(true);

		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(
				listenerConnectionFactory);
		ExecutorService mockExec = mock(ExecutorService.class);
		cachingConnectionFactory.setExecutor(mockExec);
		final CachingConnectionFactory cachingTemplateConnectionFactory = new CachingConnectionFactory(
				templateConnectionFactory);
		cachingTemplateConnectionFactory.setExecutor(mockExec);

		given(listenerConnectionFactory.newConnection(any(ExecutorService.class), anyString()))
				.willReturn(listenerConnection);
		given(listenerConnection.isOpen()).willReturn(true);
		given(templateConnectionFactory.newConnection(any(ExecutorService.class), anyString()))
				.willReturn(templateConnection);
		given(templateConnection.isOpen()).willReturn(true);
		given(templateConnection.createChannel()).willReturn(templateChannel);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();

		willAnswer(ensureOneChannelAnswer(listenerChannel, tooManyChannels)).given(listenerConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final CountDownLatch consumerLatch = new CountDownLatch(1);

		willAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumerLatch.countDown();
			return "consumerTag";
		}).given(listenerChannel)
			.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));

		final CountDownLatch commitLatch = new CountDownLatch(2);
		willAnswer(invocation -> {
			commitLatch.countDown();
			return null;
		}).given(listenerChannel).txCommit();
		willAnswer(invocation -> {
			commitLatch.countDown();
			return null;
		}).given(templateChannel).txCommit();

		final CountDownLatch latch = new CountDownLatch(1);
		AbstractMessageListenerContainer container = createContainer(cachingConnectionFactory);
		container.setMessageListener((MessageListener) message -> {
			RabbitTemplate rabbitTemplate = new RabbitTemplate(cachingTemplateConnectionFactory);
			rabbitTemplate.setChannelTransacted(true);
			// should use same channel as container
			rabbitTemplate.convertAndSend("foo", "bar", "baz");
			latch.countDown();
		});
		container.setQueueNames("queue");
		container.setChannelTransacted(true);
		container.setShutdownTimeout(100);
		container.setTransactionManager(new DummyTxManager());
		container.afterPropertiesSet();
		container.start();
		assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(), new byte[] {0});

		assertTrue(latch.await(10, TimeUnit.SECONDS));

		Exception e = tooManyChannels.get();
		if (e != null) {
			throw e;
		}

		verify(listenerConnection, Mockito.times(1)).createChannel();
		verify(templateConnection, Mockito.times(1)).createChannel();
		assertTrue(commitLatch.await(10, TimeUnit.SECONDS));
		verify(listenerChannel).txCommit();
		verify(templateChannel).basicPublish(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean(),
				Mockito.any(BasicProperties.class), Mockito.any(byte[].class));
		verify(templateChannel).txCommit();

		// verify close() was never called on the channel
		DirectFieldAccessor dfa = new DirectFieldAccessor(cachingConnectionFactory);
		List<?> channels = (List<?>) dfa.getPropertyValue("cachedChannelsTransactional");
		assertEquals(0, channels.size());

		container.stop();

	}

	/**
	 * Verifies that an up-stack RabbitTemplate uses the listener's
	 * channel (ChannelAwareMessageListener).
	 */
	@Test
	public void testChannelAwareMessageListener() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		final Channel onlyChannel = mock(Channel.class);
		given(onlyChannel.isOpen()).willReturn(true);

		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		singleConnectionFactory.setExecutor(mock(ExecutorService.class));

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();

		willAnswer(ensureOneChannelAnswer(onlyChannel, tooManyChannels)).given(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final CountDownLatch consumerLatch = new CountDownLatch(1);

		willAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumerLatch.countDown();
			return "consumerTag";
		}).given(onlyChannel)
			.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));

		final CountDownLatch commitLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			commitLatch.countDown();
			return null;
		}).given(onlyChannel).txCommit();

		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<Channel> exposed = new AtomicReference<Channel>();
		AbstractMessageListenerContainer container = createContainer(singleConnectionFactory);
		container.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
			exposed.set(channel);
			RabbitTemplate rabbitTemplate = new RabbitTemplate(singleConnectionFactory);
			rabbitTemplate.setChannelTransacted(true);
			// should use same channel as container
			rabbitTemplate.convertAndSend("foo", "bar", "baz");
			latch.countDown();
		});
		container.setQueueNames("queue");
		container.setChannelTransacted(true);
		container.setShutdownTimeout(100);
		container.setTransactionManager(new DummyTxManager());
		container.afterPropertiesSet();
		container.start();
		assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(), new byte[] {0});

		assertTrue(latch.await(10, TimeUnit.SECONDS));

		Exception e = tooManyChannels.get();
		if (e != null) {
			throw e;
		}

		verify(mockConnection, Mockito.times(1)).createChannel();
		assertTrue(commitLatch.await(10, TimeUnit.SECONDS));
		verify(onlyChannel).txCommit();
		verify(onlyChannel).basicPublish(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean(),
				Mockito.any(BasicProperties.class), Mockito.any(byte[].class));

		// verify close() was never called on the channel
		verify(onlyChannel, Mockito.never()).close();

		container.stop();

		assertSame(onlyChannel, exposed.get());
	}

	/**
	 * Verifies that an up-stack RabbitTemplate uses the listener's
	 * channel (ChannelAwareMessageListener). exposeListenerChannel=false
	 * is ignored (ChannelAwareMessageListener).
	 */
	@Test
	public void testChannelAwareMessageListenerDontExpose() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		final Channel onlyChannel = mock(Channel.class);
		given(onlyChannel.isOpen()).willReturn(true);

		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory(mockConnectionFactory);
		singleConnectionFactory.setExecutor(mock(ExecutorService.class));

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();

		willAnswer(ensureOneChannelAnswer(onlyChannel, tooManyChannels)).given(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final CountDownLatch consumerLatch = new CountDownLatch(1);

		willAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumerLatch.countDown();
			return "consumerTag";
		}).given(onlyChannel)
			.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));

		final CountDownLatch commitLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			commitLatch.countDown();
			return null;
		}).given(onlyChannel).txCommit();

		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<Channel> exposed = new AtomicReference<Channel>();
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
			exposed.set(channel);
			RabbitTemplate rabbitTemplate = new RabbitTemplate(singleConnectionFactory);
			rabbitTemplate.setChannelTransacted(true);
			// should use same channel as container
			rabbitTemplate.convertAndSend("foo", "bar", "baz");
			latch.countDown();
		});
		container.setQueueNames("queue");
		container.setChannelTransacted(true);
		container.setExposeListenerChannel(false);
		container.setShutdownTimeout(100);
		container.setTransactionManager(new DummyTxManager());
		container.afterPropertiesSet();
		container.start();
		assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(), new byte[] {0});

		assertTrue(latch.await(10, TimeUnit.SECONDS));

		Exception e = tooManyChannels.get();
		if (e != null) {
			throw e;
		}

		verify(mockConnection, Mockito.times(1)).createChannel();
		assertTrue(commitLatch.await(10, TimeUnit.SECONDS));
		verify(onlyChannel).txCommit();
		verify(onlyChannel).basicPublish(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean(),
				Mockito.any(BasicProperties.class), Mockito.any(byte[].class));

		// verify close() was never called on the channel
		verify(onlyChannel, Mockito.never()).close();

		container.stop();

		assertSame(onlyChannel, exposed.get());
	}

	/**
	 * Verifies the proper channel is bound when using a RabbitTransactionManager.
	 * Previously, the wrong channel was bound. See AMQP-260.
	 * @throws Exception
	 */
	@Test
	public void testMessageListenerWithRabbitTxManager() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		final Channel onlyChannel = mock(Channel.class);
		given(onlyChannel.isOpen()).willReturn(true);

		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(mockConnectionFactory);
		cachingConnectionFactory.setExecutor(mock(ExecutorService.class));

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString())).willReturn(mockConnection);
		given(mockConnection.isOpen()).willReturn(true);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();

		willAnswer(ensureOneChannelAnswer(onlyChannel, tooManyChannels)).given(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final CountDownLatch consumerLatch = new CountDownLatch(1);

		willAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumerLatch.countDown();
			return "consumerTag";
		}).given(onlyChannel)
				.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
						any(Consumer.class));

		final CountDownLatch commitLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			commitLatch.countDown();
			return null;
		}).given(onlyChannel).txCommit();

		final CountDownLatch latch = new CountDownLatch(1);
		AbstractMessageListenerContainer container = createContainer(cachingConnectionFactory);
		container.setMessageListener((MessageListener) message -> {
			RabbitTemplate rabbitTemplate = new RabbitTemplate(cachingConnectionFactory);
			rabbitTemplate.setChannelTransacted(true);
			// should use same channel as container
			rabbitTemplate.convertAndSend("foo", "bar", "baz");
			latch.countDown();
		});
		container.setQueueNames("queue");
		container.setChannelTransacted(true);
		container.setShutdownTimeout(100);
		container.setTransactionManager(new RabbitTransactionManager(cachingConnectionFactory));
		container.afterPropertiesSet();
		container.start();
		assertTrue(consumerLatch.await(10, TimeUnit.SECONDS));

		consumer.get().handleDelivery("qux", new Envelope(1, false, "foo", "bar"), new BasicProperties(), new byte[] {0});

		assertTrue(latch.await(10, TimeUnit.SECONDS));

		Exception e = tooManyChannels.get();
		if (e != null) {
			throw e;
		}

		verify(mockConnection, Mockito.times(1)).createChannel();
		assertTrue(commitLatch.await(10, TimeUnit.SECONDS));
		verify(onlyChannel).txCommit();
		verify(onlyChannel).basicPublish(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean(),
				Mockito.any(BasicProperties.class), Mockito.any(byte[].class));

		// verify close() was never called on the channel
		DirectFieldAccessor dfa = new DirectFieldAccessor(cachingConnectionFactory);
		List<?> channels = (List<?>) dfa.getPropertyValue("cachedChannelsTransactional");
		assertEquals(0, channels.size());

		container.stop();
	}

	private Answer<Channel> ensureOneChannelAnswer(final Channel onlyChannel,
			final AtomicReference<Exception> tooManyChannels) {
		final AtomicBoolean done = new AtomicBoolean();
		return invocation -> {
			if (!done.get()) {
				done.set(true);
				return onlyChannel;
			}
			tooManyChannels.set(new Exception("More than one channel requested"));
			Channel channel = mock(Channel.class);
			given(channel.isOpen()).willReturn(true);
			return channel;
		};
	}

	protected abstract AbstractMessageListenerContainer createContainer(AbstractConnectionFactory connectionFactory);

	@SuppressWarnings("serial")
	private static class DummyTxManager extends AbstractPlatformTransactionManager {

		private volatile boolean committed;

		private volatile boolean rolledBack;

		private volatile CountDownLatch latch = new CountDownLatch(1);

		@Override
		protected Object doGetTransaction() throws TransactionException {
			return new Object();
		}

		@Override
		protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
		}

		@Override
		protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
			this.committed = true;
			this.latch.countDown();
		}

		@Override
		protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
			this.rolledBack = true;
			this.latch.countDown();
		}
	}

}
