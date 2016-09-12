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

package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.transaction.RabbitTransactionManager;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
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
	@SuppressWarnings("unchecked")
	@Test
	public void testMessageListener() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		final Channel onlyChannel = mock(Channel.class);
		when(onlyChannel.isOpen()).thenReturn(true);

		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(mockConnectionFactory);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();

		doAnswer(new Answer<Channel>() {
			boolean done;
			@Override
			public Channel answer(InvocationOnMock invocation) throws Throwable {
				if (!done) {
					done = true;
					return onlyChannel;
				}
				tooManyChannels.set(new Exception("More than one channel requested"));
				Channel channel = mock(Channel.class);
				when(channel.isOpen()).thenReturn(true);
				return channel;
			}
		}).when(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final CountDownLatch consumerLatch = new CountDownLatch(1);

		doAnswer(invocation -> {
			consumer.set((Consumer) invocation.getArguments()[6]);
			consumerLatch.countDown();
			return "consumerTag";
		}).when(onlyChannel)
			.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));

		final CountDownLatch commitLatch = new CountDownLatch(1);
		doAnswer(invocation -> {
			commitLatch.countDown();
			return null;
		}).when(onlyChannel).txCommit();

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
		DirectFieldAccessor dfa = new DirectFieldAccessor(cachingConnectionFactory);
		List<?> channels = (List<?>) dfa.getPropertyValue("cachedChannelsTransactional");
		assertEquals(0, channels.size());

		container.stop();

	}

	/**
	 * Verifies that the channel is rolled back after an exception.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testMessageListenerRollback() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		final Channel channel = mock(Channel.class);
		when(channel.isOpen()).thenReturn(true);

		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(mockConnectionFactory);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();

		doAnswer(invocation -> channel).when(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final CountDownLatch consumerLatch = new CountDownLatch(1);

		doAnswer(invocation -> {
			consumer.set((Consumer) invocation.getArguments()[6]);
			consumerLatch.countDown();
			return "consumerTag";
		}).when(channel)
				.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
						any(Consumer.class));

		final CountDownLatch rollbackLatch = new CountDownLatch(1);
		doAnswer(invocation -> {
			rollbackLatch.countDown();
			return null;
		}).when(channel).txRollback();

		final CountDownLatch latch = new CountDownLatch(1);
		AbstractMessageListenerContainer container = createContainer(cachingConnectionFactory);
		container.setMessageListener(message -> {
			latch.countDown();
			throw new RuntimeException("force rollback");
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
		assertTrue(rollbackLatch.await(10, TimeUnit.SECONDS));
		container.stop();
	}

	/**
	 * Verifies that an up-stack RabbitTemplate does not use the listener's
	 * channel when it has its own connection factory.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testMessageListenerTemplateUsesDifferentConnectionFactory() throws Exception {
		ConnectionFactory listenerConnectionFactory = mock(ConnectionFactory.class);
		ConnectionFactory templateConnectionFactory = mock(ConnectionFactory.class);
		Connection listenerConnection = mock(Connection.class);
		Connection templateConnection = mock(Connection.class);
		final Channel listenerChannel = mock(Channel.class);
		Channel templateChannel = mock(Channel.class);
		when(listenerChannel.isOpen()).thenReturn(true);
		when(templateChannel.isOpen()).thenReturn(true);

		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(
				listenerConnectionFactory);
		final CachingConnectionFactory cachingTemplateConnectionFactory = new CachingConnectionFactory(
				templateConnectionFactory);

		when(listenerConnectionFactory.newConnection((ExecutorService) null)).thenReturn(listenerConnection);
		when(listenerConnection.isOpen()).thenReturn(true);
		when(templateConnectionFactory.newConnection((ExecutorService) null)).thenReturn(templateConnection);
		when(templateConnection.isOpen()).thenReturn(true);
		when(templateConnection.createChannel()).thenReturn(templateChannel);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();

		doAnswer(new Answer<Channel>() {
			boolean done;
			@Override
			public Channel answer(InvocationOnMock invocation) throws Throwable {
				if (!done) {
					done = true;
					return listenerChannel;
				}
				tooManyChannels.set(new Exception("More than one channel requested"));
				Channel channel = mock(Channel.class);
				when(channel.isOpen()).thenReturn(true);
				return channel;
			}
		}).when(listenerConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final CountDownLatch consumerLatch = new CountDownLatch(1);

		doAnswer(invocation -> {
			consumer.set((Consumer) invocation.getArguments()[6]);
			consumerLatch.countDown();
			return "consumerTag";
		}).when(listenerChannel)
			.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));

		final CountDownLatch commitLatch = new CountDownLatch(2);
		doAnswer(invocation -> {
			commitLatch.countDown();
			return null;
		}).when(listenerChannel).txCommit();
		doAnswer(invocation -> {
			commitLatch.countDown();
			return null;
		}).when(templateChannel).txCommit();

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
	@SuppressWarnings("unchecked")
	@Test
	public void testChannelAwareMessageListener() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		final Channel onlyChannel = mock(Channel.class);
		when(onlyChannel.isOpen()).thenReturn(true);

		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory(mockConnectionFactory);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();

		doAnswer(new Answer<Channel>() {
			boolean done;
			@Override
			public Channel answer(InvocationOnMock invocation) throws Throwable {
				if (!done) {
					done = true;
					return onlyChannel;
				}
				tooManyChannels.set(new Exception("More than one channel requested"));
				Channel channel = mock(Channel.class);
				when(channel.isOpen()).thenReturn(true);
				return channel;
			}
		}).when(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final CountDownLatch consumerLatch = new CountDownLatch(1);

		doAnswer(invocation -> {
			consumer.set((Consumer) invocation.getArguments()[6]);
			consumerLatch.countDown();
			return "consumerTag";
		}).when(onlyChannel)
			.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));

		final CountDownLatch commitLatch = new CountDownLatch(1);
		doAnswer(invocation -> {
			commitLatch.countDown();
			return null;
		}).when(onlyChannel).txCommit();

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
	@SuppressWarnings("unchecked")
	@Test
	public void testChannelAwareMessageListenerDontExpose() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		final Channel onlyChannel = mock(Channel.class);
		when(onlyChannel.isOpen()).thenReturn(true);

		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory(mockConnectionFactory);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();

		doAnswer(new Answer<Channel>() {
			boolean done;
			@Override
			public Channel answer(InvocationOnMock invocation) throws Throwable {
				if (!done) {
					done = true;
					return onlyChannel;
				}
				tooManyChannels.set(new Exception("More than one channel requested"));
				Channel channel = mock(Channel.class);
				when(channel.isOpen()).thenReturn(true);
				return channel;
			}
		}).when(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final CountDownLatch consumerLatch = new CountDownLatch(1);

		doAnswer(invocation -> {
			consumer.set((Consumer) invocation.getArguments()[6]);
			consumerLatch.countDown();
			return "consumerTag";
		}).when(onlyChannel)
			.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));

		final CountDownLatch commitLatch = new CountDownLatch(1);
		doAnswer(invocation -> {
			commitLatch.countDown();
			return null;
		}).when(onlyChannel).txCommit();

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
	@SuppressWarnings("unchecked")
	@Test
	public void testMessageListenerWithRabbitTxManager() throws Exception {
		ConnectionFactory mockConnectionFactory = mock(ConnectionFactory.class);
		Connection mockConnection = mock(Connection.class);
		final Channel onlyChannel = mock(Channel.class);
		when(onlyChannel.isOpen()).thenReturn(true);

		final CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(mockConnectionFactory);

		when(mockConnectionFactory.newConnection((ExecutorService) null)).thenReturn(mockConnection);
		when(mockConnection.isOpen()).thenReturn(true);

		final AtomicReference<Exception> tooManyChannels = new AtomicReference<Exception>();

		doAnswer(new Answer<Channel>() {
			boolean done;
			@Override
			public Channel answer(InvocationOnMock invocation) throws Throwable {
				if (!done) {
					done = true;
					return onlyChannel;
				}
				tooManyChannels.set(new Exception("More than one channel requested"));
				Channel channel = mock(Channel.class);
				when(channel.isOpen()).thenReturn(true);
				return channel;
			}
		}).when(mockConnection).createChannel();

		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final CountDownLatch consumerLatch = new CountDownLatch(1);

		doAnswer(invocation -> {
			consumer.set((Consumer) invocation.getArguments()[6]);
			consumerLatch.countDown();
			return "consumerTag";
		}).when(onlyChannel)
			.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));

		final CountDownLatch commitLatch = new CountDownLatch(1);
		doAnswer(invocation -> {
			commitLatch.countDown();
			return null;
		}).when(onlyChannel).txCommit();

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

	protected abstract AbstractMessageListenerContainer createContainer(AbstractConnectionFactory connectionFactory);

	@SuppressWarnings("serial")
	private static class DummyTxManager extends AbstractPlatformTransactionManager {

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
	}

}
