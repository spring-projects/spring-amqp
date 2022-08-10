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

package org.springframework.amqp.rabbit.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.with;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
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

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.BatchMessageListener;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.CacheMode;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
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
 * @author Mohammad Hewedy
 * @author Yansong Ren
 */
public class SimpleMessageListenerContainerTests {

	@Test
	public void testChannelTransactedOverriddenWhenTxManager() {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setMessageListener(new MessageListenerAdapter(this));
		container.setQueueNames("foo");
		container.setChannelTransacted(false);
		container.setTransactionManager(new TestTransactionManager());
		container.afterPropertiesSet();
		assertThat(TestUtils.getPropertyValue(container, "transactional", Boolean.class)).isTrue();
		container.stop();
		singleConnectionFactory.destroy();
	}

	@Test
	public void testInconsistentTransactionConfiguration() {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setMessageListener(new MessageListenerAdapter(this));
		container.setQueueNames("foo");
		container.setChannelTransacted(false);
		container.setAcknowledgeMode(AcknowledgeMode.NONE);
		container.setTransactionManager(new TestTransactionManager());
		assertThatIllegalStateException()
			.isThrownBy(container::afterPropertiesSet);
		container.stop();
		singleConnectionFactory.destroy();
	}

	@Test
	public void testInconsistentAcknowledgeConfiguration() {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setMessageListener(new MessageListenerAdapter(this));
		container.setQueueNames("foo");
		container.setChannelTransacted(true);
		container.setAcknowledgeMode(AcknowledgeMode.NONE);
		assertThatIllegalStateException()
			.isThrownBy(container::afterPropertiesSet);
		container.stop();
		singleConnectionFactory.destroy();
	}

	@Test
	public void testDefaultConsumerCount() {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory);
		container.setMessageListener(new MessageListenerAdapter(this));
		container.setQueueNames("foo");
		container.setAutoStartup(false);
		container.setShutdownTimeout(0);
		container.afterPropertiesSet();
		assertThat(ReflectionTestUtils.getField(container, "concurrentConsumers")).isEqualTo(1);
		container.stop();
		singleConnectionFactory.destroy();
	}

	@Test
	public void testLazyConsumerCount() {
		final SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory("localhost");
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(singleConnectionFactory) {

			@Override
			protected void doStart() {
				// do nothing
			}
		};
		container.start();
		assertThat(ReflectionTestUtils.getField(container, "concurrentConsumers")).isEqualTo(1);
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
		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(false)).willReturn(channel);
		final AtomicReference<Consumer> consumer = new AtomicReference<>();
		willAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumer.get().handleConsumeOk("1");
			return "1";
		}).given(channel)
				.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
						any(Consumer.class));
		final CountDownLatch latch = new CountDownLatch(2);
		willAnswer(invocation -> {
			latch.countDown();
			return null;
		}).given(channel).basicAck(anyLong(), anyBoolean());

		final List<Message> messages = new ArrayList<>();
		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foo");
		container.setBatchSize(2);
		container.setMessageListener(messages::add);
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
		assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(messages).hasSize(4);
		Executors.newSingleThreadExecutor().execute(container::stop);
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
		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(false)).willReturn(channel);
		final AtomicReference<Consumer> consumer = new AtomicReference<>();
		final String consumerTag = "1";
		willAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumer.get().handleConsumeOk(consumerTag);
			return consumerTag;
		}).given(channel)
				.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
						any(Consumer.class));
		final CountDownLatch latch = new CountDownLatch(2);
		willAnswer(invocation -> {
			latch.countDown();
			return null;
		}).given(channel).basicAck(anyLong(), anyBoolean());

		final List<Message> messages = new ArrayList<>();
		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foobar");
		container.setBatchSize(2);
		container.setMessageListener(messages::add);
		container.setShutdownTimeout(0);
		container.afterPropertiesSet();
		container.start();
		BasicProperties props = new BasicProperties();
		byte[] payload = "baz".getBytes();
		Envelope envelope = new Envelope(1L, false, "foo", "bar");
		consumer.get().handleDelivery(consumerTag, envelope, props, payload);
		envelope = new Envelope(2L, false, "foo", "bar");
		consumer.get().handleDelivery(consumerTag, envelope, props, payload);
		envelope = new Envelope(3L, false, "foo", "bar");
		consumer.get().handleDelivery(consumerTag, envelope, props, payload);
		assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(messages).hasSize(3);
		assertThat(messages.get(0).getMessageProperties().getConsumerTag()).isEqualTo(consumerTag);
		assertThat(messages.get(0).getMessageProperties().getConsumerQueue()).isEqualTo("foobar");
		Executors.newSingleThreadExecutor().execute(container::stop);
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
		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(false)).willReturn(channel);
		final AtomicReference<Consumer> consumer = new AtomicReference<>();
		final AtomicReference<Map<?, ?>> args = new AtomicReference<>();
		willAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumer.get().handleConsumeOk("foo");
			args.set(invocation.getArgument(5));
			return "foo";
		}).given(channel)
				.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), any(Map.class),
						any(Consumer.class));

		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foo");
		container.setMessageListener(message -> {
		});
		container.setConsumerArguments(Collections.singletonMap("x-priority", 10));
		container.setShutdownTimeout(0);
		container.afterPropertiesSet();
		container.start();
		verify(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
				any(Map.class),
				any(Consumer.class));
		assertThat(args.get() != null).isTrue();
		assertThat(args.get().get("x-priority")).isEqualTo(10);
		consumer.get().handleCancelOk("foo");
		container.stop();
	}

	@Test
	public void testChangeQueues() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel1 = mock(Channel.class);
		Channel channel2 = mock(Channel.class);
		given(channel1.isOpen()).willReturn(true);
		given(channel2.isOpen()).willReturn(true);
		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(false)).willReturn(channel1, channel2);
		List<Consumer> consumers = new ArrayList<>();
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
		container.setMessageListener(message -> {
		});
		container.setShutdownTimeout(0);
		container.afterPropertiesSet();
		container.start();
		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		container.addQueueNames("bar");
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		verify(channel1).basicCancel("0");
		verify(channel2, atLeastOnce()).basicCancel("1");
		verify(channel2, atLeastOnce()).basicCancel("2");
	}

	@Test
	public void testChangeQueuesSimple() {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foo");
		List<?> queues = TestUtils.getPropertyValue(container, "queues", List.class);
		assertThat(queues).hasSize(1);
		container.addQueueNames(new AnonymousQueue().getName(), new AnonymousQueue().getName());
		assertThat(queues).hasSize(3);
		container.removeQueues(new Queue("foo"));
		assertThat(queues).hasSize(2);
		container.stop();
	}

	@Test
	public void testAddQueuesAndStartInCycle() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel1 = mock(Channel.class);
		given(channel1.isOpen()).willReturn(true);
		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(false)).willReturn(channel1);
		final AtomicInteger count = new AtomicInteger();
		willAnswer(invocation -> {
			Consumer cons = invocation.getArgument(6);
			String consumerTag = "consFoo" + count.incrementAndGet();
			cons.handleConsumeOk(consumerTag);
			return consumerTag;
		}).given(channel1)
				.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
						any(Consumer.class));

		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener(message -> {
		});
		container.setShutdownTimeout(0);
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

		willAnswer(invocation -> {
			Consumer cons = invocation.getArgument(6);
			consumers.add(cons);
			String actualTag = String.valueOf(consumerTag.getAndIncrement());
			cons.handleConsumeOk(actualTag);
			latch.countDown();
			return actualTag;
		}).given(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
				any(Consumer.class));
	}

	protected void setUpMockCancel(Channel channel, final List<Consumer> consumers) throws IOException {
		final Executor exec = Executors.newCachedThreadPool();
		willAnswer(invocation -> {
			final String consTag = invocation.getArgument(0);
			exec.execute(() -> consumers.get(Integer.parseInt(consTag)).handleCancelOk(consTag));
			return null;
		}).given(channel).basicCancel(anyString());
	}

	@Test
	public void testWithConnectionPerListenerThread() throws Exception {
		com.rabbitmq.client.ConnectionFactory mockConnectionFactory =
				mock(com.rabbitmq.client.ConnectionFactory.class);
		com.rabbitmq.client.Connection mockConnection1 = mock(com.rabbitmq.client.Connection.class);
		com.rabbitmq.client.Connection mockConnection2 = mock(com.rabbitmq.client.Connection.class);
		Channel mockChannel1 = mock(Channel.class);
		Channel mockChannel2 = mock(Channel.class);

		given(mockConnectionFactory.newConnection(any(ExecutorService.class), anyString()))
				.willReturn(mockConnection1)
				.willReturn(mockConnection2)
				.willReturn(null);
		given(mockConnection1.createChannel()).willReturn(mockChannel1).willReturn(null);
		given(mockConnection2.createChannel()).willReturn(mockChannel2).willReturn(null);
		given(mockChannel1.isOpen()).willReturn(true);
		given(mockConnection1.isOpen()).willReturn(true);
		given(mockChannel2.isOpen()).willReturn(true);
		given(mockConnection2.isOpen()).willReturn(true);

		CachingConnectionFactory ccf = new CachingConnectionFactory(mockConnectionFactory);
		ccf.setExecutor(mock(ExecutorService.class));
		ccf.setCacheMode(CacheMode.CONNECTION);

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(ccf);
		container.setConcurrentConsumers(2);
		container.setQueueNames("foo");
		container.setConsumeDelay(100);
		container.afterPropertiesSet();

		CountDownLatch latch1 = new CountDownLatch(2);
		CountDownLatch latch2 = new CountDownLatch(2);
		willAnswer(messageToConsumer(mockChannel1, container, false, latch1))
				.given(mockChannel1).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
				anyMap(), any(Consumer.class));
		willAnswer(messageToConsumer(mockChannel2, container, false, latch1))
				.given(mockChannel2).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
				anyMap(), any(Consumer.class));
		willAnswer(messageToConsumer(mockChannel1, container, true, latch2)).given(mockChannel1).basicCancel(anyString());
		willAnswer(messageToConsumer(mockChannel2, container, true, latch2)).given(mockChannel2).basicCancel(anyString());

		container.start();
		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		Set<?> consumers = TestUtils.getPropertyValue(container, "consumers", Set.class);
		Iterator<?> iterator = consumers.iterator();
		Set<Long> delays = new HashSet<>();
		delays.add(100L);
		delays.add(200L);
		Long consumerDelay1 = TestUtils.getPropertyValue(iterator.next(), "consumeDelay", Long.class);
		Long consumerDelay2 = TestUtils.getPropertyValue(iterator.next(), "consumeDelay", Long.class);
		assertThat(delays).contains(consumerDelay1);
		delays.remove(consumerDelay1);
		assertThat(delays).contains(consumerDelay2);

		container.stop();
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();

		waitForConsumersToStop(consumers);
		Set<?> allocatedConnections = TestUtils.getPropertyValue(ccf, "allocatedConnections", Set.class);
		assertThat(allocatedConnections).hasSize(2);
		assertThat(ccf.getCacheProperties().get("openConnections")).isEqualTo("1");
	}

	@Test
	public void testConsumerCancel() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(false)).willReturn(channel);
		final AtomicReference<Consumer> consumer = new AtomicReference<>();
		willAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumer.get().handleConsumeOk("foo");
			return "foo";
		}).given(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
				any(Consumer.class));

		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foo");
		container.setMessageListener(message -> {
		});
		container.setReceiveTimeout(1);
		container.afterPropertiesSet();
		container.start();
		verify(channel).basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
				any(Consumer.class));
		Log logger = spy(TestUtils.getPropertyValue(container, "logger", Log.class));
		willReturn(false).given(logger).isDebugEnabled();
		willReturn(true).given(logger).isWarnEnabled();
		final CountDownLatch latch = new CountDownLatch(1);
		final List<String> messages = new ArrayList<>();
		willAnswer(invocation -> {
			String message = invocation.getArgument(0);
			messages.add(message);
			if (message.startsWith("Consumer raised exception")) {
				latch.countDown();
			}
			return invocation.callRealMethod();
		}).given(logger).warn(any());
		new DirectFieldAccessor(container).setPropertyValue("logger", logger);
		consumer.get().handleCancel("foo");
		assertThat(latch.await(10, TimeUnit.SECONDS))
				.as("Expected 'Consumer raised exception' but got %s", messages)
				.isTrue();
		container.stop();
	}

	@Test
	public void testContainerNotRecoveredAfterExhaustingRecoveryBackOff() throws Exception {
		SimpleMessageListenerContainer container =
				spy(new SimpleMessageListenerContainer(mock(ConnectionFactory.class)));
		container.setQueueNames("foo");
		container.setRecoveryBackOff(new FixedBackOff(100, 3));
		container.setConcurrentConsumers(3);
		willAnswer(invocation -> {
			BlockingQueueConsumer consumer = spy((BlockingQueueConsumer) invocation.callRealMethod());
			willThrow(RuntimeException.class).given(consumer).start();
			return consumer;
		}).given(container).createBlockingQueueConsumer();
		container.afterPropertiesSet();
		container.start();

		// Since backOff exhausting makes listenerContainer as invalid (calls stop()),
		// it is enough to check the listenerContainer activity
		await().until(() -> !container.isActive());
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

		assertThat(container.isActive()).isTrue();
		assertThat(container.isRunning()).isTrue();

		container.destroy();
	}

	@Test
	public void testNullMPP() {
		class Container extends SimpleMessageListenerContainer {

			@Override
			public void executeListener(Channel channel, Object messageIn) {
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

	@Test
	public void testChildClassLoader() {
		ClassLoader child = new URLClassLoader(new URL[0], SimpleMessageListenerContainerTests.class.getClassLoader());
		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(child);

			ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
			container.setConnectionFactory(connectionFactory);
			container.setAdviceChain((MethodInterceptor) MethodInvocation::proceed);
			container.afterPropertiesSet();

			Object proxy = TestUtils.getPropertyValue(container, "proxy");
			assertThat(AopUtils.isAopProxy(proxy)).isTrue();
		}
		finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
	}

	@Test
	public void testAddAndRemoveAfterReceivePostProcessors() {
		class Container extends SimpleMessageListenerContainer {

			@Override
			public void executeListener(Channel channel, Object messageIn) {
				super.executeListener(channel, messageIn);
			}

		}
		class DoNothingMPP implements MessagePostProcessor {

			@Override
			public Message postProcessMessage(Message message) throws AmqpException {
				return message;
			}
		}
		Container container = new Container();

		DoNothingMPP mpp1 = new DoNothingMPP();
		DoNothingMPP mpp2 = new DoNothingMPP();
		DoNothingMPP mpp3 = new DoNothingMPP();

		container.addAfterReceivePostProcessors(mpp1, mpp2);
		container.addAfterReceivePostProcessors(mpp3);
		boolean removed = container.removeAfterReceivePostProcessor(mpp1);
		@SuppressWarnings("unchecked")
		Collection<Object> afterReceivePostProcessors =
				(Collection<Object>) ReflectionTestUtils.getField(container, "afterReceivePostProcessors");

		assertThat(removed).isEqualTo(true);
		assertThat(afterReceivePostProcessors).containsExactly(mpp2, mpp3);
	}

	@SuppressWarnings("unchecked")
	@Test
	void setConcurrency() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(false)).willReturn(channel);
		final AtomicReference<Consumer> consumer = new AtomicReference<>();
		willAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumer.get().handleConsumeOk("1");
			return "1";
		}).given(channel)
				.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
						any(Consumer.class));
		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setQueueNames("foo", "bar");
		container.setMessageListener(mock(MessageListener.class));
		container.setConcurrency("5-10");
		container.start();
		await().until(() -> TestUtils.getPropertyValue(container, "consumers", Collection.class).size() == 5);
		container.setConcurrency("10-10");
		assertThat(TestUtils.getPropertyValue(container, "consumers", Collection.class)).hasSize(10);
	}

	@Test
	void filterMppNoDoubleAck() throws Exception {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(false)).willReturn(channel);
		final AtomicReference<Consumer> consumer = new AtomicReference<>();
		willAnswer(invocation -> {
			consumer.set(invocation.getArgument(6));
			consumer.get().handleConsumeOk("1");
			return "1";
		}).given(channel)
				.basicConsume(anyString(), anyBoolean(), anyString(), anyBoolean(), anyBoolean(), anyMap(),
						any(Consumer.class));
		final CountDownLatch latch = new CountDownLatch(1);
		willAnswer(invocation -> {
			latch.countDown();
			return null;
		}).given(channel).basicAck(anyLong(), anyBoolean());

		final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setAfterReceivePostProcessors(msg -> null);
		container.setQueueNames("foo");
		MessageListener listener = mock(BatchMessageListener.class);
		container.setMessageListener(listener);
		container.setBatchSize(2);
		container.setConsumerBatchEnabled(true);
		container.start();
		BasicProperties props = new BasicProperties();
		byte[] payload = "baz".getBytes();
		Envelope envelope = new Envelope(1L, false, "foo", "bar");
		consumer.get().handleDelivery("1", envelope, props, payload);
		envelope = new Envelope(2L, false, "foo", "bar");
		consumer.get().handleDelivery("1", envelope, props, payload);
		assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		verify(channel, never()).basicAck(eq(1), anyBoolean());
		verify(channel).basicAck(2, true);
		container.stop();
		verify(listener).containerAckMode(AcknowledgeMode.AUTO);
		verify(listener).isAsyncReplies();
		verifyNoMoreInteractions(listener);
	}

	@Test
	void testWithConsumerStartWhenNotActive() {
		ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(connectionFactory.createConnection()).willReturn(connection);
		given(connection.createChannel(false)).willReturn(channel);

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		// overwrite task execute. shutdown container before task execute.
		TestExecutor testExecutor = new TestExecutor(container);
		container.setTaskExecutor(testExecutor);
		container.start();

		// then add queue for trigger container shutdown
		container.addQueueNames("bar");

		// valid the 'start' countdown is 0.  lastTask is AsyncMessageProcessingConsumer
		Runnable lastTask = testExecutor.getLastTask();
		CountDownLatch start = TestUtils.getPropertyValue(lastTask, "start", CountDownLatch.class);

		assertThat(start.getCount()).isEqualTo(0L);
	}

	private Answer<Object> messageToConsumer(final Channel mockChannel, final SimpleMessageListenerContainer container,
			final boolean cancel, final CountDownLatch latch) {
		return invocation -> {
			String returnValue = null;
			Set<?> consumers = TestUtils.getPropertyValue(container, "consumers", Set.class);
			for (Object consumer : consumers) {
				ChannelProxy channel = TestUtils.getPropertyValue(consumer, "channel", ChannelProxy.class);
				if (channel != null && channel.getTargetChannel() == mockChannel) {
					if (cancel) {
						((Consumer) TestUtils.getPropertyValue(consumer, "consumers", Map.class)
								.values().iterator().next()).handleCancelOk(invocation.getArgument(0));
					}
					else {
						((Consumer) invocation.getArgument(6)).handleConsumeOk("foo");
						returnValue = "foo";
					}
					latch.countDown();
				}
			}
			return returnValue;
		};

	}

	private void waitForConsumersToStop(Set<?> consumers) throws Exception {
		with().pollInterval(Duration.ofMillis(10)).atMost(Duration.ofSeconds(10))
				.until(() -> consumers.stream()
						.map(consumer -> TestUtils.getPropertyValue(consumer, "consumer"))
						.allMatch(c -> c == null));
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

	@SuppressWarnings("serial")
	private class TestExecutor extends SimpleAsyncTaskExecutor {

		private final SimpleMessageListenerContainer simpleMessageListenerContainer;

		private int shutdownCount = 0;

		private Runnable lastTask = null;

		private TestExecutor(SimpleMessageListenerContainer simpleMessageListenerContainer) {
			this.simpleMessageListenerContainer = simpleMessageListenerContainer;
		}

		public Runnable getLastTask() {
			return lastTask;
		}

		@Override
		public void execute(Runnable task) {
			// skip the first execution
			if (++shutdownCount > 1) {
				lastTask = task;
				// before execute, shutdown the container for test
				this.simpleMessageListenerContainer.shutdown();
			}
			super.execute(task);
		}
	}

}
