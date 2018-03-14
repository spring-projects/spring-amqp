/*
 * Copyright 2016-2018 the original author or authors.
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

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.aopalliance.intercept.MethodInterceptor;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Level;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer.ChannelHolder;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.adapter.ReplyingMessageListener;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.support.ArgumentBuilder;
import org.springframework.amqp.rabbit.test.LogLevelAdjuster;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.MultiValueMap;
import org.springframework.util.backoff.FixedBackOff;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Alex Panchenko
 *
 * @since 2.0
 *
 */
public class DirectMessageListenerContainerIntegrationTests {

	private static final String Q1 = "testQ1";

	private static final String Q2 = "testQ2";

	private static final String EQ1 = "eventTestQ1";

	private static final String EQ2 = "eventTestQ2";

	private static final String DLQ1 = "testDLQ1";

	@ClassRule
	public static BrokerRunning brokerRunning = BrokerRunning.isRunningWithEmptyQueues(Q1, Q2, EQ1, EQ2, DLQ1);

	private static CachingConnectionFactory adminCf =
			new CachingConnectionFactory(brokerRunning.getConnectionFactory());

	private static RabbitAdmin admin = new RabbitAdmin(adminCf);

	@Rule
	public LogLevelAdjuster adjuster = new LogLevelAdjuster(Level.DEBUG,
			CachingConnectionFactory.class, DirectReplyToMessageListenerContainer.class,
			DirectMessageListenerContainer.class, DirectMessageListenerContainerIntegrationTests.class,
			BrokerRunning.class);

	@Rule
	public TestName testName = new TestName();

	@AfterClass
	public static void tearDown() {
		brokerRunning.removeTestQueues();
		adminCf.destroy();
	}

	@Test
	public void testSimple() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setThreadNamePrefix("client-");
		executor.afterPropertiesSet();
		cf.setExecutor(executor);
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		container.setQueueNames(Q1, Q2);
		container.setConsumersPerQueue(2);
		container.setMessageListener(new MessageListenerAdapter((ReplyingMessageListener<String, String>) in -> {
			if ("foo".equals(in) || "bar".equals(in)) {
				return in.toUpperCase();
			}
			else {
				return null;
			}
		}));
		container.setBeanName("simple");
		container.setConsumerTagStrategy(new Tag());
		container.afterPropertiesSet();
		container.start();
		RabbitTemplate template = new RabbitTemplate(cf);
		assertEquals("FOO", template.convertSendAndReceive(Q1, "foo"));
		assertEquals("BAR", template.convertSendAndReceive(Q2, "bar"));
		container.stop();
		assertTrue(consumersOnQueue(Q1, 0));
		assertTrue(consumersOnQueue(Q2, 0));
		assertTrue(activeConsumerCount(container, 0));
		assertEquals(0, TestUtils.getPropertyValue(container, "consumersByQueue", MultiValueMap.class).size());
		template.stop();
		cf.destroy();
	}

	@Test
	public void testAdvice() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		container.setQueueNames(Q1, Q2);
		container.setConsumersPerQueue(2);
		final CountDownLatch latch = new CountDownLatch(2);
		container.setMessageListener(m -> latch.countDown());
		final CountDownLatch adviceLatch = new CountDownLatch(2);
		MethodInterceptor advice = i -> {
			adviceLatch.countDown();
			return i.proceed();
		};
		container.setAdviceChain(advice);
		container.setBeanName("advice");
		container.setConsumerTagStrategy(new Tag());
		container.afterPropertiesSet();
		container.start();
		RabbitTemplate template = new RabbitTemplate(cf);
		template.convertAndSend(Q1, "foo");
		template.convertAndSend(Q1, "bar");
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertTrue(adviceLatch.await(10, TimeUnit.SECONDS));
		container.stop();
		assertTrue(consumersOnQueue(Q1, 0));
		assertTrue(consumersOnQueue(Q2, 0));
		assertTrue(activeConsumerCount(container, 0));
		assertEquals(0, TestUtils.getPropertyValue(container, "consumersByQueue", MultiValueMap.class).size());
		cf.destroy();
	}

	@Test
	public void testQueueManagement() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setThreadNamePrefix("client-");
		executor.afterPropertiesSet();
		cf.setExecutor(executor);
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		container.setConsumersPerQueue(2);
		container.setMessageListener(new MessageListenerAdapter((ReplyingMessageListener<String, String>) in -> {
			if ("foo".equals(in) || "bar".equals(in)) {
				return in.toUpperCase();
			}
			else {
				return null;
			}
		}));
		container.setBeanName("qManage");
		container.setConsumerTagStrategy(new Tag());
		container.afterPropertiesSet();
		container.start();
		container.addQueueNames(Q1, Q2);
		assertTrue(consumersOnQueue(Q1, 2));
		assertTrue(consumersOnQueue(Q2, 2));
		RabbitTemplate template = new RabbitTemplate(cf);
		assertEquals("FOO", template.convertSendAndReceive(Q1, "foo"));
		assertEquals("BAR", template.convertSendAndReceive(Q2, "bar"));
		container.removeQueueNames(Q1, Q2, "junk");
		assertTrue(consumersOnQueue(Q1, 0));
		assertTrue(consumersOnQueue(Q2, 0));
		assertTrue(activeConsumerCount(container, 0));
		container.stop();
		assertTrue(consumersOnQueue(Q1, 0));
		assertTrue(consumersOnQueue(Q2, 0));
		assertTrue(activeConsumerCount(container, 0));
		assertEquals(0, TestUtils.getPropertyValue(container, "consumersByQueue", MultiValueMap.class).size());
		template.stop();
		cf.destroy();
	}

	@Test
	public void testQueueManagementQueueInstances() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setThreadNamePrefix("client-");
		executor.afterPropertiesSet();
		cf.setExecutor(executor);
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		container.setConsumersPerQueue(2);
		container.setMessageListener(new MessageListenerAdapter((ReplyingMessageListener<String, String>) in -> {
			if ("foo".equals(in) || "bar".equals(in)) {
				return in.toUpperCase();
			}
			else {
				return null;
			}
		}));
		container.setBeanName("qManage");
		container.setConsumerTagStrategy(new Tag());
		container.afterPropertiesSet();
		container.setQueues(new Queue(Q1));
		assertArrayEquals(new String[] { Q1 }, container.getQueueNames());
		container.start();
		container.addQueues(new Queue(Q2));
		assertTrue(consumersOnQueue(Q1, 2));
		assertTrue(consumersOnQueue(Q2, 2));
		RabbitTemplate template = new RabbitTemplate(cf);
		assertEquals("FOO", template.convertSendAndReceive(Q1, "foo"));
		assertEquals("BAR", template.convertSendAndReceive(Q2, "bar"));
		container.removeQueues(new Queue(Q1), new Queue(Q2), new Queue("junk"));
		assertTrue(consumersOnQueue(Q1, 0));
		assertTrue(consumersOnQueue(Q2, 0));
		assertTrue(activeConsumerCount(container, 0));
		container.stop();
		assertTrue(consumersOnQueue(Q1, 0));
		assertTrue(consumersOnQueue(Q2, 0));
		assertTrue(activeConsumerCount(container, 0));
		assertEquals(0, TestUtils.getPropertyValue(container, "consumersByQueue", MultiValueMap.class).size());
		template.stop();
		cf.destroy();
	}

	@Test
	public void testAddRemoveConsumers() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setThreadNamePrefix("client-");
		executor.afterPropertiesSet();
		cf.setExecutor(executor);
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		container.setQueueNames(Q1, Q2);
		container.setConsumersPerQueue(4);
		container.setMessageListener(new MessageListenerAdapter((ReplyingMessageListener<String, String>) in -> {
			if ("foo".equals(in) || "bar".equals(in)) {
				return in.toUpperCase();
			}
			else {
				return null;
			}
		}));
		container.setBeanName("qAddRemove");
		container.setConsumerTagStrategy(new Tag());
		container.afterPropertiesSet();
		container.start();
		RabbitTemplate template = new RabbitTemplate(cf);
		assertEquals("FOO", template.convertSendAndReceive(Q1, "foo"));
		assertEquals("BAR", template.convertSendAndReceive(Q2, "bar"));
		assertTrue(consumersOnQueue(Q1, 4));
		assertTrue(consumersOnQueue(Q2, 4));
		container.setConsumersPerQueue(1);
		assertTrue(consumersOnQueue(Q1, 1));
		assertTrue(consumersOnQueue(Q2, 1));
		container.setConsumersPerQueue(2);
		assertTrue(consumersOnQueue(Q1, 2));
		assertTrue(consumersOnQueue(Q2, 2));
		container.stop();
		assertTrue(consumersOnQueue(Q1, 0));
		assertTrue(consumersOnQueue(Q2, 0));
		assertTrue(activeConsumerCount(container, 0));
		assertEquals(0, TestUtils.getPropertyValue(container, "consumersByQueue", MultiValueMap.class).size());
		template.stop();
		cf.destroy();
	}

	@Test
	public void testEvents() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		container.setQueueNames(EQ1, EQ2);
		final List<Long> times = new ArrayList<>();
		final CountDownLatch latch1 = new CountDownLatch(2);
		final CountDownLatch latch2 = new CountDownLatch(2);
		container.setApplicationEventPublisher(event -> {
			if (event instanceof ListenerContainerIdleEvent) {
				times.add(System.currentTimeMillis());
				latch1.countDown();
			}
			else if (event instanceof ListenerContainerConsumerTerminatedEvent) {
				latch2.countDown();
			}
		});
		container.setMessageListener(m -> { });
		container.setIdleEventInterval(50L);
		container.setBeanName("events");
		container.setConsumerTagStrategy(new Tag());
		container.afterPropertiesSet();
		container.start();
		assertTrue(latch1.await(10, TimeUnit.SECONDS));
		assertThat(times.get(1) - times.get(0), greaterThanOrEqualTo(50L));
		brokerRunning.deleteQueues(EQ1, EQ2);
		assertTrue(latch2.await(10, TimeUnit.SECONDS));
		container.stop();
		cf.destroy();
	}

	@Test
	public void testErrorHandler() throws Exception {
		brokerRunning.deleteQueues(Q1);
		Queue q1 = new Queue(Q1, true, false, false, new ArgumentBuilder()
				.put("x-dead-letter-exchange", "")
				.put("x-dead-letter-routing-key", DLQ1)
				.get());
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		RabbitAdmin admin = new RabbitAdmin(cf);
		admin.declareQueue(q1);
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		container.setQueueNames(Q1);
		container.setConsumersPerQueue(2);
		final AtomicReference<Channel> channel = new AtomicReference<>();
		container.setMessageListener((ChannelAwareMessageListener) (m, c) -> {
			channel.set(c);
			throw new MessageConversionException("intended - should be wrapped in an AmqpRejectAndDontRequeueException");
		});
		container.setBeanName("errorHandler");
		container.setConsumerTagStrategy(new Tag());
		container.afterPropertiesSet();
		container.start();
		RabbitTemplate template = new RabbitTemplate(cf);
		template.convertAndSend(Q1, "foo");
		assertNotNull(template.receive(DLQ1, 10000));
		container.stop();
		assertTrue(consumersOnQueue(Q1, 0));
		assertTrue(consumersOnQueue(Q2, 0));
		assertTrue(activeConsumerCount(container, 0));
		assertEquals(0, TestUtils.getPropertyValue(container, "consumersByQueue", MultiValueMap.class).size());
		assertFalse(channel.get().isOpen());
		cf.destroy();
	}

	@Test
	public void testContainerNotRecoveredAfterExhaustingRecoveryBackOff() throws Exception {
		ConnectionFactory mockCF = mock(ConnectionFactory.class);
		given(mockCF.createConnection()).willThrow(new RuntimeException("intended - backOff test"));
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(mockCF);
		container.setQueueNames("foo");
		container.setRecoveryBackOff(new FixedBackOff(100, 3));
		container.setMissingQueuesFatal(false);
		container.setBeanName("backOff");
		container.setConsumerTagStrategy(new Tag());
		container.afterPropertiesSet();
		container.start();

		// Since backOff exhausting makes listenerContainer as invalid (calls stop()),
		// it is enough to check the listenerContainer activity
		int n = 0;
		while (container.isActive() && n++ < 100) {
			Thread.sleep(100);
		}
		assertFalse(container.isActive());
	}

	@Test
	public void testRecoverBrokerLoss() throws Exception {
		ConnectionFactory mockCF = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(connection.isOpen()).willReturn(true);
		given(mockCF.createConnection()).willReturn(connection);
		given(connection.createChannel(false)).willReturn(channel);
		given(channel.isOpen()).willReturn(true);
		final CountDownLatch latch1 = new CountDownLatch(1);
		final CountDownLatch latch2 = new CountDownLatch(2);
		ArgumentCaptor<Consumer> consumerCaptor = ArgumentCaptor.forClass(Consumer.class);
		final String tag = "tag";
		willAnswer(i -> {
			latch1.countDown();
			latch2.countDown();
			return tag;
		}).given(channel).basicConsume(eq("foo"), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
				anyMap(), consumerCaptor.capture());
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(mockCF);
		container.setQueueNames("foo");
		container.setBeanName("brokerLost");
		container.setConsumerTagStrategy(q -> "tag");
		container.setShutdownTimeout(1);
		container.setMonitorInterval(200);
		container.setFailedDeclarationRetryInterval(200);
		container.afterPropertiesSet();
		container.start();
		assertTrue(latch1.await(10, TimeUnit.SECONDS));
		given(channel.isOpen()).willReturn(false);
		assertTrue(latch2.await(10, TimeUnit.SECONDS));
		container.setShutdownTimeout(1);
		container.stop();
	}

	@Test
	public void testCancelConsumerBeforeConsumeOk() throws Exception {
		ConnectionFactory mockCF = mock(ConnectionFactory.class);
		Connection connection = mock(Connection.class);
		Channel channel = mock(Channel.class);
		given(connection.isOpen()).willReturn(true);
		given(mockCF.createConnection()).willReturn(connection);
		given(connection.createChannel(false)).willReturn(channel);
		given(channel.isOpen()).willReturn(true);
		final CountDownLatch latch1 = new CountDownLatch(1);
		final CountDownLatch latch2 = new CountDownLatch(1);
		ArgumentCaptor<Consumer> consumerCaptor = ArgumentCaptor.forClass(Consumer.class);
		final String tag = "tag";
		willAnswer(i -> {
			latch1.countDown();
			return tag;
		}).given(channel).basicConsume(eq("foo"), anyBoolean(), anyString(), anyBoolean(), anyBoolean(),
				anyMap(), consumerCaptor.capture());
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(mockCF);
		container.setQueueNames("foo");
		container.setBeanName("backOff");
		container.setConsumerTagStrategy(q -> "tag");
		container.setShutdownTimeout(1);
		container.afterPropertiesSet();
		container.start();
		assertTrue(latch1.await(10, TimeUnit.SECONDS));
		Consumer consumer = consumerCaptor.getValue();
		Executors.newSingleThreadExecutor().execute(() -> {
			container.stop();
			latch2.countDown();
		});
		assertTrue(latch2.await(10, TimeUnit.SECONDS));
		verify(channel).basicCancel(tag); // canceled properly even without consumeOk
		consumer.handleCancelOk(tag);
	}

	@Test
	public void testRecoverDeletedQueueAutoDeclare() throws Exception {
		testRecoverDeletedQueueGuts(true);
	}

	@Test
	public void testRecoverDeletedQueueNoAutoDeclare() throws Exception {
		testRecoverDeletedQueueGuts(false);
	}

	private void testRecoverDeletedQueueGuts(boolean autoDeclare) throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		if (autoDeclare) {
			GenericApplicationContext context = new GenericApplicationContext();
			context.getBeanFactory().registerSingleton("foo", new Queue(Q1));
			RabbitAdmin admin = new RabbitAdmin(cf);
			admin.setApplicationContext(context);
			context.getBeanFactory().registerSingleton("admin", admin);
			context.refresh();
			container.setApplicationContext(context);
		}
		container.setAutoDeclare(autoDeclare);
		container.setQueueNames(Q1, Q2);
		container.setConsumersPerQueue(2);
		container.setConsumersPerQueue(2);
		container.setMessageListener(m -> { });
		container.setFailedDeclarationRetryInterval(500);
		container.setBeanName("deleteQauto=" + autoDeclare);
		container.setConsumerTagStrategy(new Tag());
		container.afterPropertiesSet();
		container.start();
		assertTrue(consumersOnQueue(Q1, 2));
		assertTrue(consumersOnQueue(Q2, 2));
		assertTrue(activeConsumerCount(container, 4));
		brokerRunning.deleteQueues(Q1);
		assertTrue(consumersOnQueue(Q2, 2));
		assertTrue(activeConsumerCount(container, 2));
		assertTrue(restartConsumerCount(container, 2));
		RabbitAdmin admin = new RabbitAdmin(cf);
		if (!autoDeclare) {
			Thread.sleep(2000);
			admin.declareQueue(new Queue(Q1));
		}
		assertTrue(consumersOnQueue(Q1, 2));
		assertTrue(consumersOnQueue(Q2, 2));
		assertTrue(activeConsumerCount(container, 4));
		container.stop();
		assertTrue(consumersOnQueue(Q1, 0));
		assertTrue(consumersOnQueue(Q2, 0));
		assertTrue(activeConsumerCount(container, 0));
		assertEquals(0, TestUtils.getPropertyValue(container, "consumersByQueue", MultiValueMap.class).size());
		cf.destroy();
	}

	@Test
	public void testReplyToReleaseWithCancel() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		DirectReplyToMessageListenerContainer container = new DirectReplyToMessageListenerContainer(cf);
		container.setBeanName("releaseCancel");
		final CountDownLatch consumeLatch = new CountDownLatch(1);
		final CountDownLatch releaseLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof ListenerContainerConsumerTerminatedEvent) {
				releaseLatch.countDown();
			}
			else if (e instanceof ConsumeOkEvent) {
				consumeLatch.countDown();
			}
		});
		container.afterPropertiesSet();
		container.start();
		ChannelHolder channelHolder = container.getChannelHolder();
		assertTrue(consumeLatch.await(10, TimeUnit.SECONDS));
		container.releaseConsumerFor(channelHolder, true, "foo");
		assertTrue(releaseLatch.await(10, TimeUnit.SECONDS));
		container.stop();
		cf.destroy();
	}

	@Test
	public void testReplyToConsumersReduced() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		DirectReplyToMessageListenerContainer container = new DirectReplyToMessageListenerContainer(cf);
		container.setBeanName("reducing");
		container.setIdleEventInterval(500);
		container.afterPropertiesSet();
		container.start();
		ChannelHolder channelHolder = container.getChannelHolder();
		assertTrue(activeConsumerCount(container, 1));
		container.releaseConsumerFor(channelHolder, false, null);
		assertTrue(activeConsumerCount(container, 0));
		container.stop();
		cf.destroy();
	}

	@Test
	public void testNonManagedContainerDoesntStartWhenConnectionFactoryDestroyed() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		ApplicationContext context = mock(ApplicationContext.class);
		cf.setApplicationContext(context);

		cf.addConnectionListener(connection -> {
			cf.onApplicationEvent(new ContextClosedEvent(context));
			cf.destroy();
		});

		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		container.setMessageListener(m -> { });
		container.setQueueNames(Q1);
		container.setBeanName("stopAfterDestroyBeforeStart");
		container.afterPropertiesSet();
		container.start();

		int n = 0;
		while (n++ < 100 && container.isRunning()) {
			Thread.sleep(100);
		}
		assertFalse(container.isRunning());
	}

	@Test
	public void testNonManagedContainerStopsWhenConnectionFactoryDestroyed() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		ApplicationContext context = mock(ApplicationContext.class);
		cf.setApplicationContext(context);
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		final CountDownLatch latch = new CountDownLatch(1);
		container.setMessageListener(m -> {
			latch.countDown();
		});
		container.setQueueNames(Q1);
		container.setBeanName("stopAfterDestroy");
		container.setIdleEventInterval(500);
		container.setFailedDeclarationRetryInterval(500);
		container.afterPropertiesSet();
		container.start();
		new RabbitTemplate(cf).convertAndSend(Q1, "foo");
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		cf.onApplicationEvent(new ContextClosedEvent(context));
		cf.destroy();
		int n = 0;
		while (n++ < 100 && container.isRunning()) {
			Thread.sleep(100);
		}
		assertFalse(container.isRunning());
	}

	@Test
	public void testDeferredAcks() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		final CountDownLatch latch = new CountDownLatch(2);
		container.setMessageListener(m -> {
			latch.countDown();
		});
		container.setQueueNames(Q1);
		container.setBeanName("deferredAcks");
		container.setMessagesPerAck(2);
		container.afterPropertiesSet();
		container.start();
		RabbitTemplate rabbitTemplate = new RabbitTemplate(cf);
		rabbitTemplate.convertAndSend(Q1, "foo");
		rabbitTemplate.convertAndSend(Q1, "bar");
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		container.stop();
		cf.destroy();
	}

	private boolean consumersOnQueue(String queue, int expected) throws Exception {
		int n = 0;
		Properties queueProperties = admin.getQueueProperties(queue);
		LogFactory.getLog(getClass()).debug(queue + " waiting for " + expected + " : " + queueProperties);
		while (n++ < 600
				&& (queueProperties == null || !queueProperties.get(RabbitAdmin.QUEUE_CONSUMER_COUNT).equals(expected))) {
			Thread.sleep(100);
			queueProperties = admin.getQueueProperties(queue);
			LogFactory.getLog(getClass()).debug(queue + " waiting for " + expected + " : " + queueProperties);
		}
		return queueProperties.get(RabbitAdmin.QUEUE_CONSUMER_COUNT).equals(expected);
	}

	private boolean activeConsumerCount(AbstractMessageListenerContainer container, int expected) throws Exception {
		int n = 0;
		List<?> consumers = TestUtils.getPropertyValue(container, "consumers", List.class);
		while (n++ < 600 && consumers.size() != expected) {
			Thread.sleep(100);
		}
		return consumers.size() == expected;
	}

	private boolean restartConsumerCount(AbstractMessageListenerContainer container, int expected) throws Exception {
		int n = 0;
		List<?> consumers = TestUtils.getPropertyValue(container, "consumersToRestart", List.class);
		while (n++ < 600 && consumers.size() != expected) {
			Thread.sleep(100);
		}
		return consumers.size() == expected;
	}

	public class Tag implements ConsumerTagStrategy {

		private volatile int n;

		@Override
		public String createConsumerTag(String queue) {
			return queue + "/" + DirectMessageListenerContainerIntegrationTests.this.testName.getMethodName() + n++;
		}

	}
}
