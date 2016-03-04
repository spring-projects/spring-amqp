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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.support.ConsumerCancelledException;
import org.springframework.amqp.rabbit.support.PublisherCallbackChannelImpl;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.BrokerTestUtils;
import org.springframework.amqp.rabbit.test.LongRunningIntegrationTest;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.support.GenericApplicationContext;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;

/**
 * @author Dave Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.3
 *
 */
public class SimpleMessageListenerContainerIntegration2Tests {

	private static Log logger = LogFactory.getLog(SimpleMessageListenerContainerIntegration2Tests.class);

	private final Queue queue = new Queue("test.queue");

	private final Queue queue1 = new Queue("test.queue.1");

	private final RabbitTemplate template = new RabbitTemplate();

	private RabbitAdmin admin;

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunningWithEmptyQueues(queue, queue1);

	@Rule
	public LongRunningIntegrationTest longRunningIntegrationTest = new LongRunningIntegrationTest();

	private SimpleMessageListenerContainer container;

	@Before
	public void declareQueues() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);
		admin = new RabbitAdmin(connectionFactory);
		admin.deleteQueue(queue.getName());
		admin.declareQueue(queue);
		admin.deleteQueue(queue1.getName());
		admin.declareQueue(queue1);
	}

	@After
	public void clear() throws Exception {
		// Wait for broker communication to finish before trying to stop container
		Thread.sleep(300L);
		logger.debug("Shutting down at end of test");
		if (container != null) {
			container.shutdown();
		}
		((DisposableBean) template.getConnectionFactory()).destroy();
		this.brokerIsRunning.removeTestQueues();
	}

	@Test
	public void testChangeQueues() throws Exception {
		CountDownLatch latch = new CountDownLatch(30);
		container = createContainer(new MessageListenerAdapter(new PojoListener(latch)), queue.getName(), queue1.getName());
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
			template.convertAndSend(queue1.getName(), i + "foo");
		}
		container.addQueueNames(queue1.getName());
		Thread.sleep(1100); // allow current consumer to time out and terminate
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		boolean waited = latch.await(10, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);
		assertNull(template.receiveAndConvert(queue.getName()));
		assertNull(template.receiveAndConvert(queue1.getName()));
	}

	@Test
	public void testDeleteOneQueue() throws Exception {
		CountDownLatch latch = new CountDownLatch(20);
		container = createContainer(new MessageListenerAdapter(new PojoListener(latch)), queue.getName(), queue1.getName());
		ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);
		container.setApplicationEventPublisher(publisher);
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
			template.convertAndSend(queue1.getName(), i + "foo");
		}
		boolean waited = latch.await(10, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);
		BlockingQueueConsumer consumer = (BlockingQueueConsumer) TestUtils
				.getPropertyValue(container, "consumers", Map.class).keySet().iterator().next();
		admin.deleteQueue(queue1.getName());
		latch = new CountDownLatch(10);
		container.setMessageListener(new MessageListenerAdapter(new PojoListener(latch)));
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		waited = latch.await(10, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);
		BlockingQueueConsumer newConsumer = (BlockingQueueConsumer) TestUtils
				.getPropertyValue(container, "consumers", Map.class).keySet().iterator().next();
		int n = 0;
		while (n++ < 100 && newConsumer == consumer) {
			Thread.sleep(100);
			newConsumer = (BlockingQueueConsumer) TestUtils
					.getPropertyValue(container, "consumers", Map.class).keySet().iterator().next();
		}
		assertTrue("Failed to restart consumer", n < 100);
		Set<?> missingQueues = TestUtils.getPropertyValue(newConsumer, "missingQueues", Set.class);
		n = 0;
		while (n++ < 100 && missingQueues.size() == 0) {
			Thread.sleep(200);
		}
		assertTrue("Failed to detect missing queue", n < 100);
		ArgumentCaptor<ListenerContainerConsumerFailedEvent> captor = ArgumentCaptor
				.forClass(ListenerContainerConsumerFailedEvent.class);
		verify(publisher).publishEvent(captor.capture());
		ListenerContainerConsumerFailedEvent event = captor.getValue();
		assertThat(event.getThrowable(), instanceOf(ConsumerCancelledException.class));
		assertFalse(event.isFatal());
		DirectFieldAccessor dfa = new DirectFieldAccessor(newConsumer);
		dfa.setPropertyValue("lastRetryDeclaration", 0);
		dfa.setPropertyValue("retryDeclarationInterval", 100);
		admin.declareQueue(queue1);
		n = 0;
		while (n++ < 100 && missingQueues.size() > 0) {
			Thread.sleep(100);
		}
		assertTrue("Failed to redeclare missing queue", n < 100);
		latch = new CountDownLatch(20);
		container.setMessageListener(new MessageListenerAdapter(new PojoListener(latch)));
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
			template.convertAndSend(queue1.getName(), i + "foo");
		}
		waited = latch.await(10, TimeUnit.SECONDS);
		assertTrue("Timed out waiting for message", waited);
		assertNull(template.receiveAndConvert(queue.getName()));
	}

	@Test
	public void testListenFromAnonQueue() throws Exception {
		AnonymousQueue queue = new AnonymousQueue();
		CountDownLatch latch = new CountDownLatch(10);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container.setMessageListener(new MessageListenerAdapter(new PojoListener(latch)));
		container.setQueueNames(queue.getName());
		container.setConcurrentConsumers(2);
		GenericApplicationContext context = new GenericApplicationContext();
		context.getBeanFactory().registerSingleton("foo", queue);
		context.refresh();
		container.setApplicationContext(context);
		RabbitAdmin admin = new RabbitAdmin(this.template.getConnectionFactory());
		admin.setApplicationContext(context);
		container.setRabbitAdmin(admin);
		container.afterPropertiesSet();
		container.start();
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		container.stop();
		container.start();
		latch = new CountDownLatch(10);
		container.setMessageListener(new MessageListenerAdapter(new PojoListener(latch)));
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		container.stop();
	}

	@Test
	public void testExclusive() throws Exception {
		Log logger = spy(TestUtils.getPropertyValue(this.template.getConnectionFactory(), "logger", Log.class));
		doReturn(true).when(logger).isInfoEnabled();
		new DirectFieldAccessor(this.template.getConnectionFactory()).setPropertyValue("logger", logger);
		CountDownLatch latch1 = new CountDownLatch(1000);
		SimpleMessageListenerContainer container1 = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container1.setMessageListener(new MessageListenerAdapter(new PojoListener(latch1)));
		container1.setQueueNames(queue.getName());
		GenericApplicationContext context = new GenericApplicationContext();
		context.getBeanFactory().registerSingleton("foo", queue);
		context.refresh();
		container1.setApplicationContext(context);
		container1.setExclusive(true);
		container1.afterPropertiesSet();
		container1.start();
		int n = 0;
		while (n++ < 100 && container1.getActiveConsumerCount() < 1) {
			Thread.sleep(100);
		}
		assertTrue(n < 100);
		CountDownLatch latch2 = new CountDownLatch(1000);
		SimpleMessageListenerContainer container2 = new SimpleMessageListenerContainer(template.getConnectionFactory());
		container2.setMessageListener(new MessageListenerAdapter(new PojoListener(latch2)));
		container2.setQueueNames(queue.getName());
		container2.setApplicationContext(context);
		container2.setRecoveryInterval(1000);
		container2.setExclusive(true); // not really necessary, but likely people will make all consumers exclusive.
		ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);
		container2.setApplicationEventPublisher(publisher);
		container2.afterPropertiesSet();
		Log containerLogger = spy(TestUtils.getPropertyValue(container2, "logger", Log.class));
		doReturn(true).when(containerLogger).isWarnEnabled();
		new DirectFieldAccessor(container2).setPropertyValue("logger", containerLogger);
		container2.start();
		for (int i = 0; i < 1000; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		assertTrue(latch1.await(10, TimeUnit.SECONDS));
		assertEquals(1000, latch2.getCount());
		container1.stop();
		// container 2 should recover and process the next batch of messages
		for (int i = 0; i < 1000; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		assertTrue(latch2.await(10, TimeUnit.SECONDS));
		container2.stop();
		ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
		verify(logger).info(captor.capture());
		assertThat(captor.getAllValues(), contains(containsString("exclusive")));
		ArgumentCaptor<ListenerContainerConsumerFailedEvent> eventCaptor = ArgumentCaptor
				.forClass(ListenerContainerConsumerFailedEvent.class);
		verify(publisher).publishEvent(eventCaptor.capture());
		ListenerContainerConsumerFailedEvent event = eventCaptor.getValue();
		assertEquals("Consumer raised exception, attempting restart", event.getReason());
		assertFalse(event.isFatal());
		assertThat(event.getThrowable(), instanceOf(AmqpIOException.class));
		verify(containerLogger).warn(any());
	}

	@Test
	public void testInvalidListener() throws Exception {
		PojoListener delegate = new PojoListener(null);
		MessageListenerAdapter messageListenerAdapter = new MessageListenerAdapter(delegate);
		messageListenerAdapter.setDefaultListenerMethod("foo");
		this.container = createContainer(messageListenerAdapter, queue.getName());
		assertTrue(containerStoppedForAbortWithBadListener());
	}

	@Test
	public void testMissingListener() throws Exception {
		this.container = createContainer(null, queue.getName());
		assertTrue(containerStoppedForAbortWithBadListener());
	}

	@Test
	public void testRestartConsumerOnBasicQosIoException() throws Exception {
		this.template.convertAndSend(queue.getName(), "foo");

		ConnectionFactory connectionFactory = new SingleConnectionFactory("localhost", BrokerTestUtils.getPort());

		final AtomicBoolean networkGlitch = new AtomicBoolean();

		class MockChannel extends PublisherCallbackChannelImpl {

			public MockChannel(Channel delegate) {
				super(delegate);
			}

			@Override
			public void basicQos(int prefetchCount) throws IOException {
				if (networkGlitch.compareAndSet(false, true)) {
					throw new IOException("Intentional connection reset");
				}
				super.basicQos(prefetchCount);
			}

		}

		Connection connection = spy(connectionFactory.createConnection());
		when(connection.createChannel(anyBoolean())).then(new Answer<Channel>() {

			@Override
			public Channel answer(InvocationOnMock invocation) throws Throwable {
				return new MockChannel((Channel) invocation.callRealMethod());
			}

		});

		DirectFieldAccessor dfa = new DirectFieldAccessor(connectionFactory);
		dfa.setPropertyValue("connection", connection);

		CountDownLatch latch = new CountDownLatch(1);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener(new MessageListenerAdapter(new PojoListener(latch)));
		container.setQueueNames(queue.getName());
		container.setRecoveryInterval(500);
		container.afterPropertiesSet();
		container.start();

		assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertTrue(networkGlitch.get());

		container.stop();
		((DisposableBean) connectionFactory).destroy();
	}

	@Test
	public void testRestartConsumerOnConnectionLossDuringQueueDeclare() throws Exception {
		this.template.convertAndSend(queue.getName(), "foo");

		ConnectionFactory connectionFactory = new CachingConnectionFactory("localhost", BrokerTestUtils.getPort());

		final AtomicBoolean networkGlitch = new AtomicBoolean();

		class MockChannel extends PublisherCallbackChannelImpl {

			public MockChannel(Channel delegate) {
				super(delegate);
			}

			@Override
			public DeclareOk queueDeclarePassive(String queue) throws IOException {
				if (networkGlitch.compareAndSet(false, true)) {
					getConnection().close();
					throw new IOException("Intentional connection reset");
				}
				return super.queueDeclarePassive(queue);
			}

		}

		Connection connection = spy(connectionFactory.createConnection());
		when(connection.createChannel(anyBoolean())).then(new Answer<Channel>() {

			@Override
			public Channel answer(InvocationOnMock invocation) throws Throwable {
				return new MockChannel((Channel) invocation.callRealMethod());
			}

		});

		DirectFieldAccessor dfa = new DirectFieldAccessor(connectionFactory);
		dfa.setPropertyValue("connection", connection);

		CountDownLatch latch = new CountDownLatch(1);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener(new MessageListenerAdapter(new PojoListener(latch)));
		container.setQueueNames(queue.getName());
		container.setRecoveryInterval(500);
		container.afterPropertiesSet();
		container.start();

		assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertTrue(networkGlitch.get());

		container.stop();
		((DisposableBean) connectionFactory).destroy();
	}

	@Test
	public void testRestartConsumerMissingQueue() throws Exception {
		Queue queue = new AnonymousQueue();
		this.template.convertAndSend(queue.getName(), "foo");

		ConnectionFactory connectionFactory = new CachingConnectionFactory("localhost", BrokerTestUtils.getPort());

		CountDownLatch latch = new CountDownLatch(1);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener(new MessageListenerAdapter(new PojoListener(latch)));
		container.setQueues(queue);
		container.setRecoveryInterval(500);
		container.setMissingQueuesFatal(false);
		container.setDeclarationRetries(1);
		container.setFailedDeclarationRetryInterval(100);
		container.setRetryDeclarationInterval(30000);
		container.afterPropertiesSet();
		container.start();

		new RabbitAdmin(connectionFactory).declareQueue(queue);
		this.template.convertAndSend(queue.getName(), "foo");

		assertTrue(latch.await(10, TimeUnit.SECONDS));

		// verify properties propagated to consumer
		BlockingQueueConsumer consumer = (BlockingQueueConsumer) TestUtils
				.getPropertyValue(container, "consumers", Map.class).keySet().iterator().next();
		assertEquals(1, TestUtils.getPropertyValue(consumer, "declarationRetries"));
		assertEquals(100L, TestUtils.getPropertyValue(consumer, "failedDeclarationRetryInterval"));
		assertEquals(30000L, TestUtils.getPropertyValue(consumer, "retryDeclarationInterval"));

		container.stop();
		((DisposableBean) connectionFactory).destroy();
	}

	@Test
	public void stopStartInListener() throws Exception {
		final AtomicReference<SimpleMessageListenerContainer> container =
				new AtomicReference<SimpleMessageListenerContainer>();
		final CountDownLatch latch = new CountDownLatch(2);
		class StopStartListener implements MessageListener {

			boolean doneStopStart;

			@Override
			public void onMessage(Message message) {
				if (!doneStopStart) {
					container.get().stop();
					container.get().start();
					doneStopStart = true;
				}
				latch.countDown();
			}

		}
		container.set(createContainer(new StopStartListener(), this.queue.getName()));
		container.get().setShutdownTimeout(1000);
		this.template.convertAndSend(this.queue.getName(), "foo");
		this.template.convertAndSend(this.queue.getName(), "foo");
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		container.get().stop();
	}

	private boolean containerStoppedForAbortWithBadListener() throws InterruptedException {
		Log logger = spy(TestUtils.getPropertyValue(container, "logger", Log.class));
		new DirectFieldAccessor(container).setPropertyValue("logger", logger);
		this.template.convertAndSend(queue.getName(), "foo");
		int n = 0;
		while (n++ < 100 && this.container.isRunning()) {
			Thread.sleep(100);
		}
		ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
		verify(logger).error(captor.capture());
		assertThat(captor.getValue(), containsString("Stopping container from aborted consumer"));
		return !this.container.isRunning();
	}

	private SimpleMessageListenerContainer createContainer(Object listener, String... queueNames) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		if (listener != null) {
			container.setMessageListener(listener);
		}
		container.setQueueNames(queueNames);
		container.afterPropertiesSet();
		container.start();
		return container;
	}

	public static class PojoListener {

		private final AtomicInteger count = new AtomicInteger();

		private final CountDownLatch latch;

		private final boolean fail;

		public PojoListener(CountDownLatch latch) {
			this(latch, false);
		}

		public PojoListener(CountDownLatch latch, boolean fail) {
			this.latch = latch;
			this.fail = fail;
		}

		public void handleMessage(String value) {
			try {
				int counter = count.getAndIncrement();
				if (logger.isDebugEnabled() && counter % 100 == 0) {
					logger.debug("Handling: " + value + ":" + counter + " - " + latch);
				}
				if (fail) {
					throw new RuntimeException("Planned failure");
				}
			} finally {
				latch.countDown();
			}
		}
	}

}
