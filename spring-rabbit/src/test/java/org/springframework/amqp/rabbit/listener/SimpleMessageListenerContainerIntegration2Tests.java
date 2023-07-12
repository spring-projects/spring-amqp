/*
 * Copyright 2002-2023 the original author or authors.
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
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.with;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.BatchMessageListener;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.event.AmqpEvent;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.PublisherCallbackChannelImpl;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerTestUtils;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.adapter.ReplyingMessageListener;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.support.ConsumerCancelledException;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;

/**
 * @author Dave Syer
 * @author Gunnar Hillert
 * @author Gary Russell
 * @author Artem Bilan
 * @author Cao Weibo
 *
 * @since 1.3
 *
 */
@RabbitAvailable(queues = { SimpleMessageListenerContainerIntegration2Tests.TEST_QUEUE,
		SimpleMessageListenerContainerIntegration2Tests.TEST_QUEUE_1 })
//@LongRunning
public class SimpleMessageListenerContainerIntegration2Tests {

	public static final String TEST_QUEUE = "test.queue.SimpleMessageListenerContainerIntegration2Tests";

	public static final String TEST_QUEUE_1 = "test.queue.1.SimpleMessageListenerContainerIntegration2Tests";

	private static Log logger = LogFactory.getLog(SimpleMessageListenerContainerIntegration2Tests.class);

	private final ExecutorService executorService = Executors.newSingleThreadExecutor();

	private final Queue queue = new Queue(TEST_QUEUE);

	private final Queue queue1 = new Queue(TEST_QUEUE_1);

	private final RabbitTemplate template = new RabbitTemplate();

	private RabbitAdmin admin;

	private SimpleMessageListenerContainer container;

	@BeforeEach
	public void declareQueues() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setPort(BrokerTestUtils.getPort());
		template.setConnectionFactory(connectionFactory);
		admin = new RabbitAdmin(connectionFactory);
	}

	@AfterEach
	public void clear() throws Exception {
		logger.debug("Shutting down at end of test");
		if (container != null) {
			container.shutdown();
		}
		((DisposableBean) template.getConnectionFactory()).destroy();
		this.executorService.shutdown();
	}

	@Test
	public void testChangeQueues() throws Exception {
		CountDownLatch latch = new CountDownLatch(30);
		AtomicInteger restarts = new AtomicInteger();
		container = spy(createContainer(new MessageListenerAdapter(new PojoListener(latch)), false, queue.getName(),
				queue1.getName()));
		willAnswer(invocation -> {
			restarts.incrementAndGet();
			invocation.callRealMethod();
			return null;
		}).given(container).addAndStartConsumers(1);
		final CountDownLatch consumerLatch = new CountDownLatch(1);
		this.container.setApplicationEventPublisher(e -> {
			if (e instanceof AsyncConsumerStoppedEvent) {
				consumerLatch.countDown();
			}
		});
		this.container.start();
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
			template.convertAndSend(queue1.getName(), i + "foo");
		}
		container.addQueueNames(queue1.getName());
		assertThat(consumerLatch.await(10, TimeUnit.SECONDS)).isTrue();
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		boolean waited = latch.await(10, TimeUnit.SECONDS);
		assertThat(waited).as("Timed out waiting for message").isTrue();
		assertThat(template.receiveAndConvert(queue.getName())).isNull();
		assertThat(template.receiveAndConvert(queue1.getName())).isNull();
		assertThat(restarts.get()).isEqualTo(1);
	}

	@Test
	public void testChangeQueues2() throws Exception { // addQueues instead of addQueueNames
		CountDownLatch latch = new CountDownLatch(30);
		container =
				createContainer(new MessageListenerAdapter(new PojoListener(latch)), queue.getName(), queue1.getName());
		final CountDownLatch consumerLatch = new CountDownLatch(1);
		this.container.setApplicationEventPublisher(e -> {
			if (e instanceof AsyncConsumerStoppedEvent) {
				consumerLatch.countDown();
			}
		});
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
			template.convertAndSend(queue1.getName(), i + "foo");
		}
		container.addQueues(queue1);
		assertThat(consumerLatch.await(10, TimeUnit.SECONDS)).isTrue();
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		boolean waited = latch.await(10, TimeUnit.SECONDS);
		assertThat(waited).as("Timed out waiting for message").isTrue();
		assertThat(template.receiveAndConvert(queue.getName())).isNull();
		assertThat(template.receiveAndConvert(queue1.getName())).isNull();
	}

	@Test
	public void testNoQueues() throws Exception {
		CountDownLatch latch1 = new CountDownLatch(20);
		container = createContainer(new MessageListenerAdapter(new PojoListener(latch1)), (String[]) null);
		container.addQueueNames(queue.getName(), queue1.getName());
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
			template.convertAndSend(queue1.getName(), i + "foo");
		}
		boolean waited = latch1.await(10, TimeUnit.SECONDS);
		assertThat(waited).as("Timed out waiting for message").isTrue();
		assertThat(template.receiveAndConvert(queue.getName())).isNull();
		assertThat(template.receiveAndConvert(queue1.getName())).isNull();
		final AtomicReference<Object> newConsumer = new AtomicReference<Object>();
		final CountDownLatch latch2 = new CountDownLatch(1);
		container.setApplicationEventPublisher(new ApplicationEventPublisher() {

			@Override
			public void publishEvent(Object event) {
				// NOSONAR
			}

			@Override
			public void publishEvent(ApplicationEvent event) {
				if (event instanceof AsyncConsumerStartedEvent) {
					newConsumer.set(((AsyncConsumerStartedEvent) event).getConsumer());
					latch2.countDown();
				}
			}
		});
		container.removeQueueNames(queue.getName(), queue1.getName());
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(TestUtils.getPropertyValue(newConsumer.get(), "queues", String[].class).length).isEqualTo(0);
	}

	@Test
	public void testDeleteOneQueue() throws Exception {
		CountDownLatch latch = new CountDownLatch(20);
		container = createContainer(new MessageListenerAdapter(new PojoListener(latch)), false,
				queue.getName(), queue1.getName());
		container.setFailedDeclarationRetryInterval(100);
		final List<AmqpEvent> events = new ArrayList<>();
		final AtomicReference<ListenerContainerConsumerFailedEvent> eventRef = new AtomicReference<>();
		final CountDownLatch eventLatch = new CountDownLatch(8);
		container.setApplicationEventPublisher(event -> {
			if (event instanceof ListenerContainerConsumerFailedEvent) {
				eventRef.set((ListenerContainerConsumerFailedEvent) event);
			}
			if (!(event instanceof MissingQueueEvent)) {
				events.add((AmqpEvent) event);
				eventLatch.countDown();
			}
		});
		container.start();
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
			template.convertAndSend(queue1.getName(), i + "foo");
		}
		boolean waited = latch.await(10, TimeUnit.SECONDS);
		assertThat(waited).as("Timed out waiting for message").isTrue();
		Set<?> consumers = TestUtils.getPropertyValue(container, "consumers", Set.class);
		BlockingQueueConsumer consumer = (BlockingQueueConsumer) consumers.iterator().next();
		admin.deleteQueue(queue1.getName());
		latch = new CountDownLatch(10);
		container.setMessageListener(new MessageListenerAdapter(new PojoListener(latch)));
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		waited = latch.await(10, TimeUnit.SECONDS);
		assertThat(waited).as("Timed out waiting for message").isTrue();
		BlockingQueueConsumer newConsumer = await("Failed to restart consumer").until(() -> {
			try {
				return (BlockingQueueConsumer) consumers.iterator().next();
			}
			catch (NoSuchElementException e) {
				// race; hasNext() won't help
				return null;
			}
		}, newCon -> newCon != consumer);
		Set<?> missingQueues = TestUtils.getPropertyValue(newConsumer, "missingQueues", Set.class);
		with().pollInterval(Duration.ofMillis(200)).await("Failed to detect missing queue")
				.atMost(Duration.ofSeconds(20))
				.until(() -> missingQueues.size() > 0);
		assertThat(eventRef.get().getThrowable()).isInstanceOf(ConsumerCancelledException.class);
		assertThat(eventRef.get().isFatal()).isFalse();
		DirectFieldAccessor dfa = new DirectFieldAccessor(newConsumer);
		dfa.setPropertyValue("lastRetryDeclaration", 0);
		dfa.setPropertyValue("retryDeclarationInterval", 100);
		admin.declareQueue(queue1);
		await("Failed to redeclare missing queue").until(() -> missingQueues.size() == 0);
		latch = new CountDownLatch(20);
		container.setMessageListener(new MessageListenerAdapter(new PojoListener(latch)));
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
			template.convertAndSend(queue1.getName(), i + "foo");
		}
		waited = latch.await(10, TimeUnit.SECONDS);
		assertThat(waited).as("Timed out waiting for message").isTrue();
		assertThat(template.receiveAndConvert(queue.getName())).isNull();
		container.stop();
		assertThat(eventLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(events).hasSize(8);
		assertThat(events.get(0)).isInstanceOf(AsyncConsumerStartedEvent.class);
		assertThat(events.get(1)).isInstanceOf(ConsumeOkEvent.class);
		ConsumeOkEvent consumeOkEvent = (ConsumeOkEvent) events.get(1);
		assertThat(consumeOkEvent.getQueue()).isIn(this.queue.getName(), this.queue1.getName());
		assertThat(events.get(2)).isInstanceOf(ConsumeOkEvent.class);
		consumeOkEvent = (ConsumeOkEvent) events.get(2);
		assertThat(consumeOkEvent.getQueue()).isIn(this.queue.getName(), this.queue1.getName());
		assertThat(eventRef.get()).isSameAs(events.get(3));
		assertThat(events.get(4)).isInstanceOf(AsyncConsumerRestartedEvent.class);
		assertThat(events.get(5)).isInstanceOf(ConsumeOkEvent.class);
		assertThat(events.get(6)).isInstanceOf(ConsumeOkEvent.class);
		assertThat(events.get(7)).isInstanceOf(AsyncConsumerStoppedEvent.class);
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
		container.setAmqpAdmin(admin);
		container.afterPropertiesSet();
		container.start();
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		container.start();
		latch = new CountDownLatch(10);
		container.setMessageListener(new MessageListenerAdapter(new PojoListener(latch)));
		for (int i = 0; i < 10; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
	}

	@Test
	public void testExclusive() throws Exception {
		Log logger = spy(TestUtils.getPropertyValue(this.template.getConnectionFactory(), "logger", Log.class));
		willReturn(true).given(logger).isInfoEnabled();
		new DirectFieldAccessor(this.template.getConnectionFactory()).setPropertyValue("logger", logger);
		CountDownLatch latch1 = new CountDownLatch(1000);
		SimpleMessageListenerContainer container1 =
				new SimpleMessageListenerContainer(template.getConnectionFactory());
		container1.setMessageListener(new MessageListenerAdapter(new PojoListener(latch1)));
		container1.setQueueNames(queue.getName());
		GenericApplicationContext context = new GenericApplicationContext();
		context.getBeanFactory().registerSingleton("foo", queue);
		context.refresh();
		container1.setApplicationContext(context);
		container1.setExclusive(true);
		final CountDownLatch consumeLatch1 = new CountDownLatch(1);
		container1.setApplicationEventPublisher(event -> {
			if (event instanceof ConsumeOkEvent) {
				consumeLatch1.countDown();
			}
		});
		container1.afterPropertiesSet();
		container1.start();
		assertThat(consumeLatch1.await(10, TimeUnit.SECONDS)).isTrue();
		CountDownLatch latch2 = new CountDownLatch(1000);
		SimpleMessageListenerContainer container2 =
				new SimpleMessageListenerContainer(template.getConnectionFactory());
		container2.setMessageListener(new MessageListenerAdapter(new PojoListener(latch2)));
		container2.setQueueNames(queue.getName());
		container2.setApplicationContext(context);
		container2.setRecoveryInterval(1000);
		container2.setExclusive(true); // not really necessary, but likely people will make all consumers exclusive.
		final AtomicReference<ListenerContainerConsumerFailedEvent> eventRef = new AtomicReference<>();
		final CountDownLatch consumeLatch2 = new CountDownLatch(1);
		container2.setApplicationEventPublisher(event -> {
			if (event instanceof ListenerContainerConsumerFailedEvent) {
				eventRef.set((ListenerContainerConsumerFailedEvent) event);
			}
			else if (event instanceof ConsumeOkEvent) {
				consumeLatch2.countDown();
			}
		});
		container2.afterPropertiesSet();
		Log containerLogger = spy(TestUtils.getPropertyValue(container2, "logger", Log.class));
		willReturn(true).given(containerLogger).isWarnEnabled();
		new DirectFieldAccessor(container2).setPropertyValue("logger", containerLogger);
		container2.start();
		for (int i = 0; i < 1000; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(latch2.getCount()).isEqualTo(1000);
		container1.stop();
		// container 2 should recover and process the next batch of messages
		assertThat(consumeLatch2.await(10, TimeUnit.SECONDS)).isTrue();
		for (int i = 0; i < 1000; i++) {
			template.convertAndSend(queue.getName(), i + "foo");
		}
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		container2.stop();
		ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
		verify(logger, atLeastOnce()).info(captor.capture());
		assertThat(captor.getAllValues()).anyMatch(arg -> arg.contains("exclusive"));
		assertThat(eventRef.get().getReason()).isEqualTo("Consumer raised exception, attempting restart");
		assertThat(eventRef.get().isFatal()).isFalse();
		assertThat(eventRef.get().getThrowable()).isInstanceOf(AmqpIOException.class);
		verify(containerLogger, atLeastOnce()).warn(any());
	}

	@Test
	public void testMissingListener() throws Exception {
		this.container = createContainer(null, queue.getName());
		assertThat(containerStoppedForAbortWithBadListener()).isTrue();
	}

	@Test
	public void testRestartConsumerOnBasicQosIoException() throws Exception {
		this.template.convertAndSend(queue.getName(), "foo");

		ConnectionFactory connectionFactory = new SingleConnectionFactory("localhost", BrokerTestUtils.getPort());

		final AtomicBoolean networkGlitch = new AtomicBoolean();

		final AtomicBoolean globalQos = new AtomicBoolean();

		class MockChannel extends PublisherCallbackChannelImpl {

			MockChannel(Channel delegate) {
				super(delegate, SimpleMessageListenerContainerIntegration2Tests.this.executorService);
			}

			@Override
			public void basicQos(int prefetchCount, boolean global) throws IOException {
				globalQos.set(global);
				if (networkGlitch.compareAndSet(false, true)) {
					throw new IOException("Intentional connection reset");
				}
				super.basicQos(prefetchCount, global);
			}

		}

		Connection connection = spy(connectionFactory.createConnection());
		given(connection.createChannel(anyBoolean()))
			.willAnswer(invocation -> new MockChannel((Channel) invocation.callRealMethod()));

		DirectFieldAccessor dfa = new DirectFieldAccessor(connectionFactory);
		dfa.setPropertyValue("connection", connection);

		CountDownLatch latch = new CountDownLatch(1);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener(new MessageListenerAdapter(new PojoListener(latch)));
		container.setQueueNames(queue.getName());
		container.setRecoveryInterval(500);
		container.setGlobalQos(true);
		container.afterPropertiesSet();
		container.start();

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(networkGlitch.get()).isTrue();
		assertThat(globalQos.get()).isTrue();

		container.stop();
		((DisposableBean) connectionFactory).destroy();
	}

	@Test
	public void testRestartConsumerOnConnectionLossDuringQueueDeclare() throws Exception {
		this.template.convertAndSend(queue.getName(), "foo");

		CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost",
				BrokerTestUtils.getPort());
		// this test closes the underlying connection normally; it will never be recovered
		connectionFactory.getRabbitConnectionFactory().setAutomaticRecoveryEnabled(false);

		final AtomicBoolean networkGlitch = new AtomicBoolean();

		class MockChannel extends PublisherCallbackChannelImpl {

			MockChannel(Channel delegate) {
				super(delegate, SimpleMessageListenerContainerIntegration2Tests.this.executorService);
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
		given(connection.createChannel(anyBoolean()))
				.willAnswer(invocation -> new MockChannel((Channel) invocation.callRealMethod()));

		DirectFieldAccessor dfa = new DirectFieldAccessor(connectionFactory);
		dfa.setPropertyValue("connection", connection);

		CountDownLatch latch = new CountDownLatch(1);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener(new MessageListenerAdapter(new PojoListener(latch)));
		container.setQueueNames(queue.getName());
		container.setRecoveryInterval(500);
		container.afterPropertiesSet();
		container.start();

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(networkGlitch.get()).isTrue();

		container.stop();
		((DisposableBean) connectionFactory).destroy();
	}

	@Test
	public void testRestartConsumerMissingQueue() throws Exception {
		Queue queue = new AnonymousQueue();
		this.template.convertAndSend(queue.getName(), "foo");

		ConnectionFactory connectionFactory = new CachingConnectionFactory("localhost", BrokerTestUtils.getPort());

		CountDownLatch latch = new CountDownLatch(1);
		CountDownLatch missingLatch = new CountDownLatch(1);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
		container.setMessageListener(new MessageListenerAdapter(new PojoListener(latch)));
		container.setQueues(queue);
		container.setRecoveryInterval(500);
		container.setMissingQueuesFatal(false);
		container.setDeclarationRetries(1);
		container.setFailedDeclarationRetryInterval(100);
		container.setRetryDeclarationInterval(30000);
		container.setApplicationEventPublisher(event -> {
			if (event instanceof MissingQueueEvent) {
				missingLatch.countDown();
			}
		});
		container.afterPropertiesSet();
		container.start();

		assertThat(missingLatch.await(10, TimeUnit.SECONDS)).isTrue();

		new RabbitAdmin(connectionFactory).declareQueue(queue);
		this.template.convertAndSend(queue.getName(), "foo");

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

		// verify properties propagated to consumer
		BlockingQueueConsumer consumer = (BlockingQueueConsumer) TestUtils
				.getPropertyValue(container, "consumers", Set.class).iterator().next();
		assertThat(TestUtils.getPropertyValue(consumer, "declarationRetries")).isEqualTo(1);
		assertThat(TestUtils.getPropertyValue(consumer, "failedDeclarationRetryInterval")).isEqualTo(100L);
		assertThat(TestUtils.getPropertyValue(consumer, "retryDeclarationInterval")).isEqualTo(30000L);

		container.stop();
		((DisposableBean) connectionFactory).destroy();
	}

	@Test
	public void stopStartInListener() throws Exception {
		AtomicReference<SimpleMessageListenerContainer> container = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(2);
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
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.get().stop();
	}

	@Test
	public void testTransientBadMessageDoesntStopContainer() throws Exception {
		CountDownLatch latch = new CountDownLatch(3);
		this.container =
				createContainer(new MessageListenerAdapter(new PojoListener(latch, false)), this.queue.getName());
		this.template.convertAndSend(this.queue.getName(), "foo");
		this.template.convertAndSend(this.queue.getName(), new Foo());
		this.template.convertAndSend(this.queue.getName(), new Bar());
		this.template.convertAndSend(this.queue.getName(), "foo");
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.container.isRunning()).isTrue();
		this.container.stop();
	}

	@Test
	public void testTransientBadMessageDoesntStopContainerLambda() throws Exception {
		final CountDownLatch latch = new CountDownLatch(2);
		this.container = createContainer(new MessageListenerAdapter(
				(ReplyingMessageListener<String, Void>) m -> {
					latch.countDown();
					return null;
				}), this.queue.getName());
		this.template.convertAndSend(this.queue.getName(), "foo");
		this.template.convertAndSend(this.queue.getName(), new Foo());
		this.template.convertAndSend(this.queue.getName(), "foo");
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.container.isRunning()).isTrue();
		this.container.stop();
	}

	@Test
	public void testTooSmallExecutor() {
		this.container = createContainer((m) -> {
		}, false, this.queue.getName());
		ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
		exec.initialize();
		this.container.setTaskExecutor(exec);
		this.container.setConcurrentConsumers(2);
		this.container.setConsumerStartTimeout(100);
		Log logger = spy(TestUtils.getPropertyValue(container, "logger", Log.class));
		new DirectFieldAccessor(container).setPropertyValue("logger", logger);
		this.container.start();
		this.container.stop();
		ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
		verify(logger).error(captor.capture());
		assertThat(captor.getValue()).isEqualTo("Consumer failed to start in 100 milliseconds; does the task "
				+ "executor have enough threads to support the container concurrency?");
	}

	@Test
	public void testErrorStopsContainer() throws Exception {
		this.container = createContainer((m) -> {
			throw new Error("testError");
		}, false, this.queue.getName());
		this.container.setjavaLangErrorHandler(error -> { });
		final CountDownLatch latch = new CountDownLatch(1);
		this.container.setApplicationEventPublisher(event -> {
			if (event instanceof ListenerContainerConsumerFailedEvent) {
				latch.countDown();
			}
		});
		this.container.setDefaultRequeueRejected(false);
		this.container.start();
		this.template.convertAndSend(this.queue.getName(), "foo");
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		await().until(() -> !this.container.isRunning());
	}

	@Test
	public void testManualAckWithClosedChannel() throws Exception {
		final AtomicReference<IllegalStateException> exc = new AtomicReference<>();
		final CountDownLatch latch = new CountDownLatch(1);
		this.container = createContainer((ChannelAwareMessageListener) (m, c) -> {
			if (exc.get() == null) {
				((CachingConnectionFactory) this.template.getConnectionFactory()).resetConnection();
			}
			try {
				c.basicAck(m.getMessageProperties().getDeliveryTag(), false);
			}
			catch (IllegalStateException e) {
				exc.set(e);
			}
			latch.countDown();
		}, false, this.queue.getName());
		this.container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
		this.container.afterPropertiesSet();
		this.container.start();
		this.template.convertAndSend(this.queue.getName(), "foo");
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		this.container.stop();
		assertThat(exc.get()).isNotNull();
		assertThat(exc.get().getMessage()).isEqualTo("Channel closed; cannot ack/nack");
	}

	@Test
	public void testMessageAckListenerWithSuccessfulAck() throws Exception {
		final AtomicInteger calledTimes = new AtomicInteger();
		final AtomicReference<Long> ackDeliveryTag = new AtomicReference<>();
		final AtomicReference<Boolean> ackSuccess = new AtomicReference<>();
		final AtomicReference<Throwable> ackCause = new AtomicReference<>();
		final CountDownLatch latch = new CountDownLatch(1);
		this.container = createContainer((ChannelAwareMessageListener) (m, c) -> {
		}, false, this.queue.getName());
		this.container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		this.container.setMessageAckListener((success, deliveryTag, cause) -> {
			calledTimes.incrementAndGet();
			ackDeliveryTag.set(deliveryTag);
			ackSuccess.set(success);
			ackCause.set(cause);
			latch.countDown();
		});
		this.container.afterPropertiesSet();
		this.container.start();
		int messageCount = 5;
		for (int i = 0; i < messageCount; i++) {
			this.template.convertAndSend(this.queue.getName(), "foo");
		}
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		this.container.stop();
		assertThat(calledTimes.get()).isEqualTo(messageCount);
		assertThat(ackSuccess.get()).isTrue();
		assertThat(ackCause.get()).isNull();
		assertThat(ackDeliveryTag.get()).isEqualTo(messageCount);
	}

	@Test
	public void testMessageAckListenerWithBatchAck() throws Exception {
		final AtomicInteger calledTimes = new AtomicInteger();
		final AtomicReference<Long> ackDeliveryTag = new AtomicReference<>();
		final AtomicReference<Boolean> ackSuccess = new AtomicReference<>();
		final AtomicReference<Throwable> ackCause = new AtomicReference<>();
		final CountDownLatch latch = new CountDownLatch(1);
		this.container = createContainer((BatchMessageListener) messages -> {
		}, false, this.queue.getName());
		this.container.setBatchSize(5);
		this.container.setConsumerBatchEnabled(true);
		this.container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		this.container.setMessageAckListener((success, deliveryTag, cause) -> {
			calledTimes.incrementAndGet();
			ackDeliveryTag.set(deliveryTag);
			ackSuccess.set(success);
			ackCause.set(cause);
			latch.countDown();
		});
		this.container.afterPropertiesSet();
		this.container.start();
		int messageCount = 5;
		for (int i = 0; i < messageCount; i++) {
			this.template.convertAndSend(this.queue.getName(), "foo");
		}
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		this.container.stop();
		assertThat(calledTimes.get()).isEqualTo(1);
		assertThat(ackSuccess.get()).isTrue();
		assertThat(ackCause.get()).isNull();
		assertThat(ackDeliveryTag.get()).isEqualTo(messageCount);
	}

	@Test
	void forceStop() {
		CountDownLatch latch1 = new CountDownLatch(1);
		this.container = createContainer((ChannelAwareMessageListener) (msg, chan) -> {
			latch1.await(10, TimeUnit.SECONDS);
		}, false, TEST_QUEUE);
		try {
			this.container.setForceStop(true);
			this.template.convertAndSend(TEST_QUEUE, "one");
			this.template.convertAndSend(TEST_QUEUE, "two");
			this.template.convertAndSend(TEST_QUEUE, "three");
			this.template.convertAndSend(TEST_QUEUE, "four");
			this.template.convertAndSend(TEST_QUEUE, "five");
			await().untilAsserted(() -> {
				QueueInformation queueInfo = admin.getQueueInfo(TEST_QUEUE);
				assertThat(queueInfo).isNotNull();
				assertThat(queueInfo.getMessageCount()).isEqualTo(5);
			});
			this.container.start();
			await().untilAsserted(() -> {
				QueueInformation queueInfo = admin.getQueueInfo(TEST_QUEUE);
				assertThat(queueInfo).isNotNull();
				assertThat(queueInfo.getMessageCount()).isEqualTo(0);
			});
			this.container.stop(() -> {
			});
			latch1.countDown();
			await().untilAsserted(() -> {
				QueueInformation queueInfo = admin.getQueueInfo(TEST_QUEUE);
				assertThat(queueInfo).isNotNull();
				assertThat(queueInfo.getMessageCount()).isEqualTo(4);
			});
		}
		finally {
			this.container.stop();
		}
	}

	private boolean containerStoppedForAbortWithBadListener() throws InterruptedException {
		Log logger = spy(TestUtils.getPropertyValue(container, "logger", Log.class));
		new DirectFieldAccessor(container).setPropertyValue("logger", logger);
		this.template.convertAndSend(queue.getName(), "foo");
		await().until(() -> !this.container.isRunning());
		ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
		verify(logger).error(captor.capture());
		assertThat(captor.getValue()).contains("Stopping container from aborted consumer");
		return !this.container.isRunning();
	}

	private SimpleMessageListenerContainer createContainer(MessageListener listener, String... queueNames) {
		return createContainer(listener, true, queueNames);
	}

	private SimpleMessageListenerContainer createContainer(MessageListener listener, boolean start,
			String... queueNames) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(template.getConnectionFactory());
		if (listener != null) {
			container.setMessageListener(listener);
		}
		if (queueNames != null) {
			container.setQueueNames(queueNames);
		}
		container.setReceiveTimeout(50);
		container.afterPropertiesSet();
		if (start) {
			container.start();
		}
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
			}
			finally {
				latch.countDown();
			}
		}

		public void handleMessage(Foo value) {
			try {
				int counter = count.getAndIncrement();
				if (logger.isDebugEnabled() && counter % 100 == 0) {
					logger.debug("Handling: " + value + ":" + counter + " - " + latch);
				}
				if (fail) {
					throw new RuntimeException("Planned failure");
				}
			}
			finally {
				latch.countDown();
			}
		}

	}

	@SuppressWarnings("serial")
	private static final class Foo implements Serializable {

		Foo() {
		}

	}

	@SuppressWarnings("serial")
	private static final class Bar implements Serializable {

		Bar() {
		}

	}

}
