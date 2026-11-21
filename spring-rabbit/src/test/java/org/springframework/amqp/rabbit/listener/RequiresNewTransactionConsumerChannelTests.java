/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.transaction.RabbitTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A channel transacted {@link RabbitTemplate} used within a
 * {@code PROPAGATION_REQUIRES_NEW} transaction in a listener must not take over the
 * consumer channel of the container: the inner transaction must neither close, commit
 * nor roll back it. This applies to an external transaction manager as well as
 * to a {@link RabbitTransactionManager}.
 *
 * @author Norbert Wirges
 *
 * @since 4.2
 */
class RequiresNewTransactionConsumerChannelTests extends AbstractTestContainerTests {

	private static final String IN = "test.requires.new.in";

	private static final String OUT = "test.requires.new.out";

	private final CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost", amqpPort());

	private final RabbitAdmin admin = new RabbitAdmin(this.connectionFactory);

	private final RabbitTemplate template = new RabbitTemplate(this.connectionFactory);

	static Stream<Arguments> containersAndTransactionManagers() {
		return Stream.of(
				Arguments.of(true, false),
				Arguments.of(true, true),
				Arguments.of(false, false),
				Arguments.of(false, true));
	}

	@BeforeEach
	void setUp() {
		// closing a channel physically closes it if the cache is full
		this.connectionFactory.setChannelCacheSize(1);
		this.admin.deleteQueue(IN);
		this.admin.deleteQueue(OUT);
		this.admin.declareQueue(new Queue(IN));
		this.admin.declareQueue(new Queue(OUT));
		this.template.setChannelTransacted(true);
	}

	@AfterEach
	void tearDown() {
		this.admin.deleteQueue(IN);
		this.admin.deleteQueue(OUT);
		this.connectionFactory.destroy();
	}

	@ParameterizedTest(name = "simple: {0}, rabbitTransactionManager: {1}")
	@MethodSource("containersAndTransactionManagers")
	void consumerChannelIsNotClosedByInnerTransaction(boolean simple, boolean rabbitTransactionManager) {
		PlatformTransactionManager transactionManager = transactionManager(rabbitTransactionManager);
		TransactionTemplate requiresNew = requiresNew(transactionManager);
		AtomicInteger deliveries = new AtomicInteger();
		AtomicInteger redeliveries = new AtomicInteger();
		AbstractMessageListenerContainer container = createContainer(simple, transactionManager,
				(message, channel) -> {
					deliveries.incrementAndGet();
					if (message.getMessageProperties().isRedelivered()) {
						redeliveries.incrementAndGet();
					}
					requiresNew.executeWithoutResult(status -> this.template.convertAndSend(OUT, "inner"));
				});
		container.start();
		try {
			await().until(() -> this.admin.getQueueInfo(IN).getConsumerCount() == 1);
			// fill the cache: the consumer holds the only other channel
			this.template.execute(channel -> null);
			new RabbitTemplate(this.connectionFactory).convertAndSend(IN, "test");

			Message received = new RabbitTemplate(this.connectionFactory).receive(OUT, 10_000);
			assertThat(received).isNotNull();
			// a closed consumer channel causes a redelivery after the consumer restart
			await().during(Duration.ofSeconds(2))
					.atMost(Duration.ofSeconds(10))
					.untilAsserted(() -> {
						assertThat(deliveries).hasValue(1);
						assertThat(redeliveries).hasValue(0);
					});
		}
		finally {
			container.stop();
		}
	}

	@ParameterizedTest(name = "simple: {0}, rabbitTransactionManager: {1}")
	@MethodSource("containersAndTransactionManagers")
	void innerTransactionDoesNotCommitWorkOfListenerTransaction(boolean simple, boolean rabbitTransactionManager)
			throws InterruptedException {

		PlatformTransactionManager transactionManager = transactionManager(rabbitTransactionManager);
		TransactionTemplate requiresNew = requiresNew(transactionManager);
		CountDownLatch latch = new CountDownLatch(1);
		AbstractMessageListenerContainer container = createContainer(simple, transactionManager,
				(message, channel) -> {
					// must be rolled back with the listener's transaction
					this.template.convertAndSend(OUT, "outer");
					requiresNew.executeWithoutResult(status -> this.template.convertAndSend(OUT, "inner"));
					latch.countDown();
					throw new AmqpRejectAndDontRequeueException("intentional: roll back the listener's transaction");
				});
		container.start();
		try {
			new RabbitTemplate(this.connectionFactory).convertAndSend(IN, "test");
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

			RabbitTemplate receiver = new RabbitTemplate(this.connectionFactory);
			Message inner = receiver.receive(OUT, 10_000);
			assertThat(inner).isNotNull();
			assertThat(inner.getBody()).asString().isEqualTo("inner");
			assertThat(receiver.receive(OUT, 1000))
					.withFailMessage("the work of the rolled back listener transaction was committed")
					.isNull();
		}
		finally {
			container.stop();
		}
	}

	@ParameterizedTest(name = "simple: {0}, rabbitTransactionManager: {1}")
	@MethodSource("containersAndTransactionManagers")
	void consumerChannelIsUsedForEachListenerTransaction(boolean simple, boolean rabbitTransactionManager)
			throws InterruptedException {

		PlatformTransactionManager transactionManager = transactionManager(rabbitTransactionManager);
		TransactionTemplate requiresNew = requiresNew(transactionManager);
		CountDownLatch latch = new CountDownLatch(2);
		List<Boolean> consumerChannelUsed = new CopyOnWriteArrayList<>();
		AbstractMessageListenerContainer container = createContainer(simple, transactionManager,
				(message, channel) -> {
					requiresNew.executeWithoutResult(status -> this.template.convertAndSend(OUT, "inner"));
					// the listener's transaction still uses the consumer channel
					consumerChannelUsed.add(this.template.execute(templateChannel -> templateChannel == channel));
					latch.countDown();
				});
		container.start();
		try {
			RabbitTemplate sender = new RabbitTemplate(this.connectionFactory);
			sender.convertAndSend(IN, "test1");
			sender.convertAndSend(IN, "test2");
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(consumerChannelUsed).containsExactly(true, true);
		}
		finally {
			container.stop();
		}
	}

	private PlatformTransactionManager transactionManager(boolean rabbitTransactionManager) {
		return rabbitTransactionManager
				? new RabbitTransactionManager(this.connectionFactory)
				: new ExternalTxManagerTests.DummyTxManager();
	}

	private static TransactionTemplate requiresNew(PlatformTransactionManager transactionManager) {
		TransactionTemplate requiresNew = new TransactionTemplate(transactionManager);
		requiresNew.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
		return requiresNew;
	}

	private AbstractMessageListenerContainer createContainer(boolean simple,
			PlatformTransactionManager transactionManager, ChannelAwareMessageListener listener) {

		AbstractMessageListenerContainer container = simple
				? new SimpleMessageListenerContainer(this.connectionFactory)
				: new DirectMessageListenerContainer(this.connectionFactory);
		container.setQueueNames(IN);
		container.setChannelTransacted(true);
		container.setTransactionManager(transactionManager);
		container.setRecoveryBackOff(new FixedBackOff(200, FixedBackOff.UNLIMITED_ATTEMPTS));
		container.setMessageListener(listener);
		container.afterPropertiesSet();
		return container;
	}

}
