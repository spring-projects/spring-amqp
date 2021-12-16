/*
 * Copyright 2016-2021 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.ArgumentCaptor;

import org.springframework.amqp.AmqpAuthenticationException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.junit.BrokerRunningSupport;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer.ChannelHolder;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.adapter.ReplyingMessageListener;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.support.ArgumentBuilder;
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
 * @author Cao Weibo
 *
 * @since 2.0
 *
 */
@RabbitAvailable(queues = { DirectMessageListenerContainerIntegrationTests.Q1,
		DirectMessageListenerContainerIntegrationTests.Q2,
		DirectMessageListenerContainerIntegrationTests.EQ1,
		DirectMessageListenerContainerIntegrationTests.EQ2,
		DirectMessageListenerContainerIntegrationTests.DLQ1 })
@LogLevels(classes = { CachingConnectionFactory.class, DirectReplyToMessageListenerContainer.class,
			DirectMessageListenerContainer.class, DirectMessageListenerContainerIntegrationTests.class,
			BrokerRunning.class }, level = "DEBUG")
public class DirectMessageListenerContainerIntegrationTests {

	public static final String Q1 = "testQ1.DirectMessageListenerContainerIntegrationTests";

	public static final String Q2 = "testQ2.DirectMessageListenerContainerIntegrationTests";

	public static final String EQ1 = "eventTestQ1.DirectMessageListenerContainerIntegrationTests";

	public static final String EQ2 = "eventTestQ2.DirectMessageListenerContainerIntegrationTests";

	public static final String DLQ1 = "testDLQ1.DirectMessageListenerContainerIntegrationTests";

	private static final String MISSING = "missing.DirectMessageListenerContainerIntegrationTests";

	private static CachingConnectionFactory adminCf;

	private static RabbitAdmin admin;

	private String testName;

	@BeforeAll
	public static void setUp() {
		adminCf = new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		admin = new RabbitAdmin(adminCf);
	}

	@AfterAll
	public static void tearDown() {
		adminCf.destroy();
	}

	@BeforeEach
	public void captureTestName(TestInfo info) {
		this.testName = info.getDisplayName();
	}

	@Test
	void authFailed() {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		cf.setUsername("junk");
		DirectMessageListenerContainer dmlc = new DirectMessageListenerContainer(cf);
		dmlc.setPossibleAuthenticationFailureFatal(true);
		assertThatExceptionOfType(AmqpAuthenticationException.class).isThrownBy(() -> dmlc.start());
	}

	@SuppressWarnings("unchecked")
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
		assertThat(template.convertSendAndReceive(Q1, "foo")).isEqualTo("FOO");
		assertThat(template.convertSendAndReceive(Q2, "bar")).isEqualTo("BAR");
		container.stop();
		assertThat(consumersOnQueue(Q1, 0)).isTrue();
		assertThat(consumersOnQueue(Q2, 0)).isTrue();
		assertThat(activeConsumerCount(container, 0)).isTrue();
		assertThat(TestUtils.getPropertyValue(container, "consumersByQueue", MultiValueMap.class)).hasSize(0);
		template.stop();
		cf.destroy();
		executor.destroy();
	}

	@Test
	public void testBadHost() throws InterruptedException {
		CachingConnectionFactory cf = new CachingConnectionFactory("this.host.does.not.exist");
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setThreadNamePrefix("client-");
		executor.afterPropertiesSet();
		cf.setExecutor(executor);
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		container.setQueueNames("dummy");
		container.setConsumersPerQueue(2);
		container.setMessageListener(in -> {
		});
		container.setBeanName("badHost");
		container.setConsumerTagStrategy(new Tag());
		CountDownLatch latch = new CountDownLatch(1);
		container.setApplicationEventPublisher(ev -> {
			if (ev instanceof ListenerContainerConsumerFailedEvent) {
				latch.countDown();
			}
		});
		container.setRecoveryInterval(100);
		container.afterPropertiesSet();
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		cf.destroy();
		executor.destroy();
	}

	@SuppressWarnings("unchecked")
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
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(adviceLatch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		assertThat(consumersOnQueue(Q1, 0)).isTrue();
		assertThat(consumersOnQueue(Q2, 0)).isTrue();
		assertThat(activeConsumerCount(container, 0)).isTrue();
		assertThat(TestUtils.getPropertyValue(container, "consumersByQueue", MultiValueMap.class)).hasSize(0);
		cf.destroy();
	}

	@SuppressWarnings("unchecked")
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
		assertThat(consumersOnQueue(Q1, 2)).isTrue();
		assertThat(consumersOnQueue(Q2, 2)).isTrue();
		RabbitTemplate template = new RabbitTemplate(cf);
		assertThat(template.convertSendAndReceive(Q1, "foo")).isEqualTo("FOO");
		assertThat(template.convertSendAndReceive(Q2, "bar")).isEqualTo("BAR");
		container.removeQueueNames(Q1, Q2, "junk");
		assertThat(consumersOnQueue(Q1, 0)).isTrue();
		assertThat(consumersOnQueue(Q2, 0)).isTrue();
		assertThat(activeConsumerCount(container, 0)).isTrue();
		container.stop();
		assertThat(consumersOnQueue(Q1, 0)).isTrue();
		assertThat(consumersOnQueue(Q2, 0)).isTrue();
		assertThat(activeConsumerCount(container, 0)).isTrue();
		assertThat(TestUtils.getPropertyValue(container, "consumersByQueue", MultiValueMap.class)).hasSize(0);
		template.stop();
		cf.destroy();
		executor.destroy();
	}

	@SuppressWarnings("unchecked")
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
		assertThat(container.getQueueNames()).isEqualTo(new String[]{Q1});
		container.start();
		container.addQueues(new Queue(Q2));
		assertThat(consumersOnQueue(Q1, 2)).isTrue();
		assertThat(consumersOnQueue(Q2, 2)).isTrue();
		RabbitTemplate template = new RabbitTemplate(cf);
		assertThat(template.convertSendAndReceive(Q1, "foo")).isEqualTo("FOO");
		assertThat(template.convertSendAndReceive(Q2, "bar")).isEqualTo("BAR");
		container.removeQueues(new Queue(Q1), new Queue(Q2), new Queue("junk"));
		assertThat(consumersOnQueue(Q1, 0)).isTrue();
		assertThat(consumersOnQueue(Q2, 0)).isTrue();
		assertThat(activeConsumerCount(container, 0)).isTrue();
		container.stop();
		assertThat(consumersOnQueue(Q1, 0)).isTrue();
		assertThat(consumersOnQueue(Q2, 0)).isTrue();
		assertThat(activeConsumerCount(container, 0)).isTrue();
		assertThat(TestUtils.getPropertyValue(container, "consumersByQueue", MultiValueMap.class)).hasSize(0);
		template.stop();
		cf.destroy();
		executor.destroy();
	}

	@SuppressWarnings("unchecked")
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
		assertThat(template.convertSendAndReceive(Q1, "foo")).isEqualTo("FOO");
		assertThat(template.convertSendAndReceive(Q2, "bar")).isEqualTo("BAR");
		assertThat(consumersOnQueue(Q1, 4)).isTrue();
		assertThat(consumersOnQueue(Q2, 4)).isTrue();
		container.setConsumersPerQueue(1);
		assertThat(consumersOnQueue(Q1, 1)).isTrue();
		assertThat(consumersOnQueue(Q2, 1)).isTrue();
		container.setConsumersPerQueue(2);
		assertThat(consumersOnQueue(Q1, 2)).isTrue();
		assertThat(consumersOnQueue(Q2, 2)).isTrue();
		container.stop();
		assertThat(consumersOnQueue(Q1, 0)).isTrue();
		assertThat(consumersOnQueue(Q2, 0)).isTrue();
		assertThat(activeConsumerCount(container, 0)).isTrue();
		assertThat(TestUtils.getPropertyValue(container, "consumersByQueue", MultiValueMap.class)).hasSize(0);
		template.stop();
		cf.destroy();
		executor.destroy();
	}

	@Test
	public void testEvents(BrokerRunningSupport brokerRunning) throws Exception {
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
		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(times.get(1) - times.get(0)).isGreaterThanOrEqualTo(50L);
		brokerRunning.deleteQueues(EQ1, EQ2);
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		cf.destroy();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testErrorHandler(BrokerRunningSupport brokerRunning) throws Exception {
		brokerRunning.deleteQueues(Q1);
		Queue q1 = new Queue(Q1, true, false, false, new ArgumentBuilder()
				.put("x-dead-letter-exchange", "")
				.put("x-dead-letter-routing-key", DLQ1)
				.get());
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		RabbitAdmin rabbitAdmin = new RabbitAdmin(cf);
		rabbitAdmin.declareQueue(q1);
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
		assertThat(template.receive(DLQ1, 10000)).isNotNull();
		container.stop();
		assertThat(consumersOnQueue(Q1, 0)).isTrue();
		assertThat(consumersOnQueue(Q2, 0)).isTrue();
		assertThat(activeConsumerCount(container, 0)).isTrue();
		assertThat(TestUtils.getPropertyValue(container, "consumersByQueue", MultiValueMap.class)).hasSize(0);
		assertThat(channel.get().isOpen()).isFalse();
		cf.destroy();
	}

	@Test
	public void testContainerNotRecoveredAfterExhaustingRecoveryBackOff() throws Exception {
		ConnectionFactory mockCF = mock(ConnectionFactory.class);
		given(mockCF.createConnection()).willReturn(null).willThrow(new RuntimeException("intended - backOff test"));
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
		await().until(() -> !container.isActive());
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
		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		given(channel.isOpen()).willReturn(false);
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
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
		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		Consumer consumer = consumerCaptor.getValue();
		ExecutorService exec = Executors.newSingleThreadExecutor();
		exec.execute(() -> {
			container.stop();
			latch2.countDown();
		});
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		verify(channel).basicCancel(tag); // canceled properly even without consumeOk
		consumer.handleCancelOk(tag);
		exec.shutdownNow();
	}

	@Test
	public void testRecoverDeletedQueueAutoDeclare(BrokerRunningSupport brokerRunning) throws Exception {
		testRecoverDeletedQueueGuts(true, brokerRunning);
	}

	@Test
	public void testRecoverDeletedQueueNoAutoDeclare(BrokerRunningSupport brokerRunning) throws Exception {
		testRecoverDeletedQueueGuts(false, brokerRunning);
	}

	@SuppressWarnings("unchecked")
	private void testRecoverDeletedQueueGuts(boolean autoDeclare, BrokerRunningSupport brokerRunning) throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		if (autoDeclare) {
			GenericApplicationContext context = new GenericApplicationContext();
			context.getBeanFactory().registerSingleton("foo", new Queue(Q1));
			RabbitAdmin rabbitAdmin = new RabbitAdmin(cf);
			rabbitAdmin.setApplicationContext(context);
			context.getBeanFactory().registerSingleton("admin", rabbitAdmin);
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
		assertThat(consumersOnQueue(Q1, 2)).isTrue();
		assertThat(consumersOnQueue(Q2, 2)).isTrue();
		assertThat(activeConsumerCount(container, 4)).isTrue();
		brokerRunning.deleteQueues(Q1);
		assertThat(consumersOnQueue(Q2, 2)).isTrue();
		assertThat(activeConsumerCount(container, 2)).isTrue();
		assertThat(restartConsumerCount(container, 2)).isTrue();
		RabbitAdmin rabbitAdmin = new RabbitAdmin(cf);
		if (!autoDeclare) {
			Thread.sleep(2000);
			rabbitAdmin.declareQueue(new Queue(Q1));
		}
		assertThat(consumersOnQueue(Q1, 2)).isTrue();
		assertThat(consumersOnQueue(Q2, 2)).isTrue();
		assertThat(activeConsumerCount(container, 4)).isTrue();
		container.stop();
		assertThat(consumersOnQueue(Q1, 0)).isTrue();
		assertThat(consumersOnQueue(Q2, 0)).isTrue();
		assertThat(activeConsumerCount(container, 0)).isTrue();
		assertThat(TestUtils.getPropertyValue(container, "consumersByQueue", MultiValueMap.class)).hasSize(0);
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
		assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		container.releaseConsumerFor(channelHolder, true, "foo");
		assertThat(releaseLatch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		cf.destroy();
	}

	@Test
	public void testReplyToConsumersReduced() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		DirectReplyToMessageListenerContainer container = new DirectReplyToMessageListenerContainer(cf);
		container.setBeanName("reducing");
		container.setIdleEventInterval(100);
		CountDownLatch latch = new CountDownLatch(5);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof ListenerContainerIdleEvent) {
				latch.countDown();
			}
		});
		container.afterPropertiesSet();
		container.start();
		ChannelHolder channelHolder1 = container.getChannelHolder();
		ChannelHolder channelHolder2 = container.getChannelHolder();
		assertThat(activeConsumerCount(container, 2)).isTrue();
		container.releaseConsumerFor(channelHolder2, false, null);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(channelHolder1.getChannel().isOpen()).isTrue();
		container.releaseConsumerFor(channelHolder1, false, null);
		assertThat(activeConsumerCount(container, 0)).isTrue();
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

		await().until(() -> !container.isActive());
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
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		cf.onApplicationEvent(new ContextClosedEvent(context));
		cf.destroy();
		await().until(() -> !container.isActive());
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
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		cf.destroy();
	}

	@Test
	void missingQueueOnStart() throws InterruptedException {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		RabbitAdmin admin = new RabbitAdmin(cf);
		admin.deleteQueue(MISSING);
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		container.setQueueNames(MISSING);
		container.setBeanName("missingQOnStart");
		final CountDownLatch latch = new CountDownLatch(1);
		container.setApplicationEventPublisher(event -> {
			if (event instanceof MissingQueueEvent) {
				admin.declareQueue(new Queue(MISSING));
			}
			else if (event instanceof AsyncConsumerStartedEvent) {
				latch.countDown();
			}
		});
		container.setMonitorInterval(500);
		container.setFailedDeclarationRetryInterval(500);
		container.afterPropertiesSet();
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		admin.deleteQueue(MISSING);
		cf.destroy();
	}

	@Test
	public void testMessageAckListenerWithSuccessfulAck() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		final AtomicInteger calledTimes = new AtomicInteger();
		final AtomicReference<Long> ackDeliveryTag = new AtomicReference<>();
		final AtomicReference<Boolean> ackSuccess = new AtomicReference<>();
		final AtomicReference<Throwable> ackCause = new AtomicReference<>();
		final CountDownLatch latch = new CountDownLatch(1);
		container.setQueueNames(Q1);
		container.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(Message message) {
			}
		});
		container.setMessageAckListener(new MessageAckListener() {
			@Override
			public void onComplete(boolean success, long deliveryTag, Throwable cause) throws Exception {
				calledTimes.incrementAndGet();
				ackDeliveryTag.set(deliveryTag);
				ackSuccess.set(success);
				ackCause.set(cause);
				latch.countDown();
			}
		});
		container.start();
		RabbitTemplate rabbitTemplate = new RabbitTemplate(cf);
		final int messageCount = 5;
		for (int i = 0; i < messageCount; i++) {
			rabbitTemplate.convertAndSend(Q1, "foo");
		}
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		assertThat(calledTimes.get()).isEqualTo(messageCount);
		assertThat(ackSuccess.get()).isTrue();
		assertThat(ackCause.get()).isNull();
		assertThat(ackDeliveryTag.get()).isEqualTo(messageCount);
	}

	@Test
	public void testMessageAckListenerWithFailedAck() throws Exception {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		final AtomicReference<Boolean> called = new AtomicReference<>();
		final AtomicReference<Long> ackDeliveryTag = new AtomicReference<>();
		final AtomicReference<Boolean> ackSuccess = new AtomicReference<>();
		final AtomicReference<Throwable> ackCause = new AtomicReference<>();
		final CountDownLatch latch = new CountDownLatch(1);
		container.setQueueNames(Q1);
		container.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(Message message) {
				cf.resetConnection();
			}
		});
		container.setMessageAckListener(new MessageAckListener() {
			@Override
			public void onComplete(boolean success, long deliveryTag, Throwable cause) throws Exception {
				called.set(true);
				ackDeliveryTag.set(deliveryTag);
				ackSuccess.set(success);
				ackCause.set(cause);
				latch.countDown();
			}
		});
		container.start();
		new RabbitTemplate(cf).convertAndSend(Q1, "foo");
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		assertThat(called.get()).isTrue();
		assertThat(ackSuccess.get()).isFalse();
		assertThat(ackCause.get().getMessage()).isEqualTo("Channel closed; cannot ack/nack");
		assertThat(ackDeliveryTag.get()).isEqualTo(1);
	}

	@Test
	public void testMessageAckListenerWithBatchAck() throws Exception {
		final AtomicInteger calledTimes = new AtomicInteger();
		final AtomicReference<Long> ackDeliveryTag = new AtomicReference<>();
		final AtomicReference<Boolean> ackSuccess = new AtomicReference<>();
		final AtomicReference<Throwable> ackCause = new AtomicReference<>();
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		DirectMessageListenerContainer container = new DirectMessageListenerContainer(cf);
		final int messageCount = 5;
		final CountDownLatch latch = new CountDownLatch(1);
		container.setQueueNames(Q1);
		container.setMessagesPerAck(messageCount);
		container.setMessageListener(message -> {
		});
		container.setMessageAckListener(new MessageAckListener() {
			@Override
			public void onComplete(boolean success, long deliveryTag, Throwable cause) throws Exception {
				calledTimes.incrementAndGet();
				ackDeliveryTag.set(deliveryTag);
				ackSuccess.set(success);
				ackCause.set(cause);
				latch.countDown();
			}
		});
		container.start();
		RabbitTemplate rabbitTemplate = new RabbitTemplate(cf);
		for (int i = 0; i < messageCount; i++) {
			rabbitTemplate.convertAndSend(Q1, "foo");
		}
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		assertThat(calledTimes.get()).isEqualTo(1);
		assertThat(ackSuccess.get()).isTrue();
		assertThat(ackCause.get()).isNull();
		assertThat(ackDeliveryTag.get()).isEqualTo(messageCount);
	}

	private boolean consumersOnQueue(String queue, int expected) throws Exception {
		await().with().pollDelay(Duration.ZERO).atMost(Duration.ofSeconds(60))
				.until(() -> admin.getQueueProperties(queue),
						props -> props != null && props.get(RabbitAdmin.QUEUE_CONSUMER_COUNT).equals(expected));
		return true;
	}

	private boolean activeConsumerCount(AbstractMessageListenerContainer container, int expected) throws Exception {
		List<?> consumers = TestUtils.getPropertyValue(container, "consumers", List.class);
		await().with().pollDelay(Duration.ZERO).atMost(Duration.ofSeconds(60))
				.until(() -> consumers.size() == expected);
		return true;
	}

	private boolean restartConsumerCount(AbstractMessageListenerContainer container, int expected) throws Exception {
		Set<?> consumers = TestUtils.getPropertyValue(container, "consumersToRestart", Set.class);
		await().with().pollDelay(Duration.ZERO).atMost(Duration.ofSeconds(60))
				.until(() -> consumers.size() == expected);
		return true;
	}

	public class Tag implements ConsumerTagStrategy {

		private volatile int n;

		@Override
		public String createConsumerTag(String queue) {
			return queue + "/" + DirectMessageListenerContainerIntegrationTests.this.testName + n++;
		}

	}
}
