/*
 * Copyright 2002-2017 the original author or authors.
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

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.stubbing.Answer;

import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.CacheMode;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.util.backoff.FixedBackOff;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.PossibleAuthenticationFailureException;


/**
 * @author David Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 * @author Artem Bilan
 */
public class SimpleMessageListenerContainerTests {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testChannelTransactedOverriddenWhenTxManager() throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setMessageListener(new MessageListenerAdapter(this));
		container.setQueueNames("foo");
		container.setChannelTransacted(false);
		container.setTransactionManager(new TestTransactionManager());
		container.afterPropertiesSet();
		assertTrue(TestUtils.getPropertyValue(container, "transactional", Boolean.class));
		container.stop();
		singleConnectionFactory.destroy();
	}

	@Test
	public void testInconsistentTransactionConfiguration() throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setMessageListener(new MessageListenerAdapter(this));
		container.setQueueNames("foo");
		container.setChannelTransacted(false);
		container.setAcknowledgeMode(AcknowledgeMode.NONE);
		container.setTransactionManager(new TestTransactionManager());
		expectedException.expect(IllegalStateException.class);
		container.afterPropertiesSet();
		container.stop();
		singleConnectionFactory.destroy();
	}

	@Test
	public void testInconsistentAcknowledgeConfiguration() throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setMessageListener(new MessageListenerAdapter(this));
		container.setQueueNames("foo");
		container.setChannelTransacted(true);
		container.setAcknowledgeMode(AcknowledgeMode.NONE);
		expectedException.expect(IllegalStateException.class);
		container.afterPropertiesSet();
		container.stop();
		singleConnectionFactory.destroy();
	}

	@Test
	public void testDefaultConsumerCount() throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setMessageListener(new MessageListenerAdapter(this));
		container.setQueueNames("foo");
		container.setAutoStartup(false);
		container.afterPropertiesSet();
		assertEquals(1, ReflectionTestUtils.getField(container, "concurrentConsumers"));
		container.stop();
		singleConnectionFactory.destroy();
	}

	@Test
	public void testLazyConsumerCount() throws Exception {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory) {
			@Override
			protected void doStart() throws Exception {
				// do nothing
			}
		};
		container.start();
		assertEquals(1, ReflectionTestUtils.getField(container, "concurrentConsumers"));
		container.stop();
		singleConnectionFactory.destroy();
	}

	/*
	 * txSize = 2; 4 messages; should get 2 acks (#2 and #4)
	 */
	@Test
	public void testTxSizeAcks() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createChannel(false)).thenReturn(channel);
		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		doAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumer.get().handleConsumeOk("1");
			return "1";
		}).when(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));
		final CountDownLatch latch = new CountDownLatch(2);
		doAnswer(invocation -> {
			latch.countDown();
			return null;
		}).when(channel).basicAck(anyLong(), anyBoolean());

		final List<Message> messages = new ArrayList<Message>();
		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foo");
		container.setTxSize(2);
		container.setMessageListener((MessageListener) message -> messages.add(message));
		container.start();
		BasicProperties props = new BasicProperties();
		byte[] payload = "baz".getBytes();
		Envelope envelope = new Envelope(1L, false, "foo", "bar");
		consumer.get().handleDelivery("1", envelope, props, payload);
		envelope = new Envelope(2L, false, "foo", "bar");
		consumer.get().handleDelivery("1", envelope, props, payload);
		envelope = new Envelope(3L, false, "foo", "bar");
		consumer.get().handleDelivery("1", envelope, props, payload);
		envelope = new Envelope(4L, false, "foo", "bar");
		consumer.get().handleDelivery("1", envelope, props, payload);
		assertTrue(latch.await(5, TimeUnit.SECONDS));
		assertEquals(4, messages.size());
		Executors.newSingleThreadExecutor().execute(() -> container.stop());
		consumer.get().handleCancelOk("1");
		verify(channel, times(2)).basicAck(anyLong(), anyBoolean());
		verify(channel).basicAck(2, true);
		verify(channel).basicAck(4, true);
		container.stop();
	}

	/*
	 * txSize = 2; 3 messages; should get 2 acks (#2 and #3)
	 * after timeout.
	 */
	@Test
	public void testTxSizeAcksWIthShortSet() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createChannel(false)).thenReturn(channel);
		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final String consumerTag = "1";
		doAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumer.get().handleConsumeOk(consumerTag);
			return consumerTag;
		}).when(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));
		final CountDownLatch latch = new CountDownLatch(2);
		doAnswer(invocation -> {
			latch.countDown();
			return null;
		}).when(channel).basicAck(anyLong(), anyBoolean());

		final List<Message> messages = new ArrayList<Message>();
		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foobar");
		container.setTxSize(2);
		container.setMessageListener((MessageListener) message -> messages.add(message));
		container.start();
		BasicProperties props = new BasicProperties();
		byte[] payload = "baz".getBytes();
		Envelope envelope = new Envelope(1L, false, "foo", "bar");
		consumer.get().handleDelivery(consumerTag, envelope, props, payload);
		envelope = new Envelope(2L, false, "foo", "bar");
		consumer.get().handleDelivery(consumerTag, envelope, props, payload);
		envelope = new Envelope(3L, false, "foo", "bar");
		consumer.get().handleDelivery(consumerTag, envelope, props, payload);
		assertTrue(latch.await(5, TimeUnit.SECONDS));
		assertEquals(3, messages.size());
		assertEquals(consumerTag, messages.get(0).getMessageProperties().getConsumerTag());
		assertEquals("foobar", messages.get(0).getMessageProperties().getConsumerQueue());
		Executors.newSingleThreadExecutor().execute(() -> container.stop());
		consumer.get().handleCancelOk(consumerTag);
		verify(channel, times(2)).basicAck(anyLong(), anyBoolean());
		verify(channel).basicAck(2, true);
		// second set was short
		verify(channel).basicAck(3, true);
		container.stop();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testConsumerArgs() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createChannel(false)).thenReturn(channel);
		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		final AtomicReference<Map<?, ?>> args = new AtomicReference<Map<?, ?>>();
		doAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumer.get().handleConsumeOk("foo");
			args.set(invocation.getArgument(5));
			return "foo";
		}).when(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), any(Map.class), any(Consumer.class));

		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foo");
		container.setMessageListener((MessageListener) message -> { });
		container.setConsumerArguments(Collections.<String, Object>singletonMap("x-priority", Integer.valueOf(10)));
		container.afterPropertiesSet();
		container.start();
		verify(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), any(Map.class),
				any(Consumer.class));
		assertTrue(args.get() != null);
		assertEquals(10, args.get().get("x-priority"));
		consumer.get().handleCancelOk("foo");
		container.stop();
	}

	@Test
	public void testChangeQueues() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel1 = mock(Channel.class);
		Channel channel2 = mock(Channel.class);
		when(channel1.isOpen()).thenReturn(true);
		when(channel2.isOpen()).thenReturn(true);
		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createChannel(false)).thenReturn(channel1, channel2);
		List<Consumer> consumers = new ArrayList<Consumer>();
		AtomicInteger consumerTag = new AtomicInteger();
		CountDownLatch latch1 = new CountDownLatch(1);
		CountDownLatch latch2 = new CountDownLatch(2);
		setupMockConsume(channel1, consumers, consumerTag, latch1);
		setUpMockCancel(channel1, consumers);
		setupMockConsume(channel2, consumers, consumerTag, latch2);
		setUpMockCancel(channel2, consumers);

		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foo");
		container.setReceiveTimeout(1);
		container.setMessageListener((MessageListener) message -> { });
		container.afterPropertiesSet();
		container.start();
		assertTrue(latch1.await(10, TimeUnit.SECONDS));
		container.addQueueNames("bar");
		assertTrue(latch2.await(10, TimeUnit.SECONDS));
		container.stop();
		verify(channel1).basicCancel("0");
		verify(channel2, atLeastOnce()).basicCancel("1");
		verify(channel2, atLeastOnce()).basicCancel("2");
	}

	@Test
	public void testChangeQueuesSimple() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foo");
		List<?> queues = TestUtils.getPropertyValue(container, "queueNames", List.class);
		assertEquals(1, queues.size());
		container.addQueues(new AnonymousQueue(), new AnonymousQueue());
		assertEquals(3, queues.size());
		container.removeQueues(new Queue("foo"));
		assertEquals(2, queues.size());
		container.stop();
	}

	@Test
	public void testAddQueuesAndStartInCycle() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel1 = mock(Channel.class);
		when(channel1.isOpen()).thenReturn(true);
		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createChannel(false)).thenReturn(channel1);
		final AtomicInteger count = new AtomicInteger();
		doAnswer(invocation -> {
			Consumer cons = invocation.getArgument(6);
			String consumerTag = "consFoo" + count.incrementAndGet();
			cons.handleConsumeOk(consumerTag);
			return consumerTag;
		}).when(channel1).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));

		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener((MessageListener) message -> { });
		container.afterPropertiesSet();

		for (int i = 0; i < 10; i++) {
			container.addQueueNames("foo" + i);
			if (!container.isRunning()) {
				container.start();
			}
		}
		container.stop();
	}

	protected void setupMockConsume(Channel channel, final List<Consumer> consumers, final AtomicInteger consumerTag,
			final CountDownLatch latch) throws IOException {
		doAnswer(invocation -> {
			Consumer cons = invocation.getArgument(6);
			consumers.add(cons);
			String actualTag = String.valueOf(consumerTag.getAndIncrement());
			cons.handleConsumeOk(actualTag);
			latch.countDown();
			return actualTag;
		}).when(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));
	}

	protected void setUpMockCancel(Channel channel, final List<Consumer> consumers) throws IOException {
		final Executor exec = Executors.newCachedThreadPool();
		doAnswer(invocation -> {
			final String consTag = invocation.getArgument(0);
			exec.execute(() -> consumers.get(Integer.parseInt(consTag)).handleCancelOk(consTag));
			return null;
		}).when(channel).basicCancel(anyString());
	}

	@Test
	public void testWithConnectionPerListenerThread() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory = mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection1 = mock(com.rabbitmq.client.Connection.class);
		com.rabbitmq.client.Connection mockConnection2 = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);

		when(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString()))
			.thenReturn(mockConnection1)
			.thenReturn(mockConnection2)
			.thenReturn(null);
		when(mockConnection1.createChannel()).thenReturn(mockChannel1).thenReturn(null);
		when(mockConnection2.createChannel()).thenReturn(mockChannel2).thenReturn(null);
		when(mockChannel1.isOpen()).thenReturn(true);
		when(mockConnection1.isOpen()).thenReturn(true);
		when(mockChannel2.isOpen()).thenReturn(true);
		when(mockConnection2.isOpen()).thenReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setCacheMode(CacheMode.CONNECTION);

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(ccf);
		container.setConcurrentConsumers(2);
		container.setQueueNames("foo");
		container.afterPropertiesSet();

		CountDownLatch latch1 = new CountDownLatch(2);
		CountDownLatch latch2 = new CountDownLatch(2);
		doAnswer(messageToConsumer(mockChannel1, container, false, latch1))
		.when(mockChannel1).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));
		doAnswer(messageToConsumer(mockChannel2, container, false, latch1))
			.when(mockChannel2).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));
		doAnswer(messageToConsumer(mockChannel1, container, true, latch2)).when(mockChannel1).basicCancel(anyString());
		doAnswer(messageToConsumer(mockChannel2, container, true, latch2)).when(mockChannel2).basicCancel(anyString());

		container.start();
		assertTrue(latch1.await(10, TimeUnit.SECONDS));
		Set<?> consumers = TestUtils.getPropertyValue(container, "consumers", Set.class);
		container.stop();
		assertTrue(latch2.await(10, TimeUnit.SECONDS));

		waitForConsumersToStop(consumers);
		Set<?> allocatedConnections = TestUtils.getPropertyValue(ccf, "allocatedConnections", Set.class);
		assertEquals(2, allocatedConnections.size());
		assertEquals("1", ccf.getCacheProperties().get("openConnections"));
	}

	@Test
	public void testConsumerCancel() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createChannel(false)).thenReturn(channel);
		final AtomicReference<Consumer> consumer = new AtomicReference<Consumer>();
		doAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumer.get().handleConsumeOk("foo");
			return "foo";
		}).when(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));

		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foo");
		container.setMessageListener((MessageListener) message -> { });
		container.setReceiveTimeout(1);
		container.afterPropertiesSet();
		container.start();
		verify(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(), any(Consumer.class));
		Log logger = spy(TestUtils.getPropertyValue(container, "logger", Log.class));
		doReturn(false).when(logger).isDebugEnabled();
		final CountDownLatch latch = new CountDownLatch(1);
		doAnswer(invocation -> {
			String message = invocation.getArgument(0);
			if (message.startsWith("Consumer raised exception")) {
				latch.countDown();
			}
			return invocation.callRealMethod();
		}).when(logger).warn(any());
		new DirectFieldAccessor(container).setPropertyValue("logger", logger);
		consumer.get().handleCancel("foo");
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		container.stop();
	}

	@Test
	public void testContainerNotRecoveredAfterExhaustingRecoveryBackOff() throws Exception {
		SimpleMessageListenerContainer container =
				spy(new SimpleMessageListenerContainer(mock(ConnectionFactory.class)));
		container.setQueueNames("foo");
		container.setRecoveryBackOff(new FixedBackOff(100, 3));
		container.setConcurrentConsumers(3);
		doAnswer(invocation -> {
			BlockingQueueConsumer consumer = spy((BlockingQueueConsumer) invocation.callRealMethod());
			doThrow(RuntimeException.class).when(consumer).start();
			return consumer;
		}).when(container).createBlockingQueueConsumer();
		container.afterPropertiesSet();
		container.start();

		// Since backOff exhausting makes listenerContainer as invalid (calls stop()),
		// it is enough to check the listenerContainer activity
		int n = 0;
		while (container.isActive() && n++ < 100) {
			Thread.sleep(100);
		}
		assertThat(n, lessThanOrEqualTo(100));
	}

	@Test
	public void testPossibleAuthenticationFailureNotFatal() {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

		given(connectionFactory.createConnection())
				.willThrow(new AmqpAuthenticationException(new PossibleAuthenticationFailureException("intentional")));

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		container.setConnectionFactory(connectionFactory);
		container.setQueueNames("foo");
		container.setPossibleAuthenticationFailureFatal(false);

		container.start();

		assertTrue(container.isActive());
		assertTrue(container.isRunning());

		container.destroy();
	}

	@Test
	public void testNullMPP() throws Exception {
		class Container extends SimpleMessageListenerContainer {

			@Override
			public void executeListener(Channel channel, Message messageIn) throws Exception {
				super.executeListener(channel, messageIn);
			}

		}
		Container container = new Container();
		container.setMessageListener(m -> {
			// NOSONAR
		});
		container.setAfterReceivePostProcessors(m -> null);
		container.setConnectionFactory(mock(ConnectionFactory.class));
		container.afterPropertiesSet();
		container.start();
		try {
			container.executeListener(null, MessageBuilder.withBody("foo".getBytes()).build());
			fail("Expected exception");
		}
		catch (ImmediateAcknowledgeAmqpException e) {
			// NOSONAR
		}
		container.stop();
	}

	private Answer<Object> messageToConsumer(final Channel mockChannel, final SimpleMessageListenerContainer container,
			final boolean cancel, final CountDownLatch latch) {
		return invocation -> {
			String returnValue = null;
			Set<?> consumers = TestUtils.getPropertyValue(container, "consumers", Set.class);
			for (Object consumer : consumers) {
				ChannelProxy channel = TestUtils.getPropertyValue(consumer, "channel", ChannelProxy.class);
				if (channel != null && channel.getTargetChannel() == mockChannel) {
					Consumer rabbitConsumer = TestUtils.getPropertyValue(consumer, "consumer", Consumer.class);
					if (cancel) {
						rabbitConsumer.handleCancelOk(invocation.getArgument(0));
					}
					else {
						rabbitConsumer.handleConsumeOk("foo");
						returnValue = "foo";
					}
					latch.countDown();
				}
			}
			return returnValue;
		};

	}

	private void waitForConsumersToStop(Set<?> consumers) throws Exception {
		int n = 0;
		boolean stillUp = true;
		while (stillUp && n++ < 1000) {
			stillUp = false;
			for (Object consumer : consumers) {
				stillUp |= TestUtils.getPropertyValue(consumer, "consumer") != null;
			}
			Thread.sleep(10);
		}
		assertFalse(stillUp);
	}

	@SuppressWarnings("serial")
	private class TestTransactionManager extends AbstractPlatformTransactionManager {

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
