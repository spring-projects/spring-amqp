/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.client.listener;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.ReconnectOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.client.AmqpClient;
import org.springframework.amqp.client.AmqpConnectionFactory;
import org.springframework.amqp.client.SpringAmqpConnectionFactory;
import org.springframework.amqp.client.config.EnableAmqp;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests for a missing queue not breaking other links on the same connection.
 *
 * @author Tanishq Choudhary
 *
 * @since 4.2
 */
@SpringJUnitConfig
@DirtiesContext
class AmqpMessageListenerContainerSessionRecoveryTests extends AbstractTestContainerTests {

	static final String HEALTHY_QUEUE = "session_recovery_healthy_queue";

	static final String MISSING_QUEUE = "session_recovery_missing_queue";

	@BeforeAll
	static void initQueues() throws IOException, InterruptedException {
		declareQueue(HEALTHY_QUEUE);
		declareQueue(MISSING_QUEUE);
	}

	static void declareQueue(String queue) throws IOException, InterruptedException {
		RABBITMQ.execInContainer("rabbitmqadmin", "queues", "declare", "--name", queue);
	}

	static void deleteQueue(String queue) throws IOException, InterruptedException {
		RABBITMQ.execInContainer("rabbitmqadmin", "queues", "delete", "--name", queue);
	}

	@Autowired
	TestConfig testConfig;

	@Autowired
	AmqpClient amqpClient;

	@Autowired
	AmqpConnectionFactory connectionFactory;

	@Test
	void missingQueueDoesNotBreakOtherLinksOnTheConnection() throws Exception {
		sendAndReceive(HEALTHY_QUEUE, "before");

		deleteQueue(MISSING_QUEUE);

		// Give the consumers of the deleted queue time to attempt a recovery against the missing queue.
		Thread.sleep(2000);

		sendAndReceive(HEALTHY_QUEUE, "after_delete");

		declareQueue(MISSING_QUEUE);

		await().atMost(Duration.ofSeconds(30))
				.untilAsserted(() -> sendAndReceive(MISSING_QUEUE, "after_redeclare"));
	}

	@Test
	void linksRecoverWhenTheSharedSessionIsClosed() throws Exception {
		sendAndReceive(HEALTHY_QUEUE, "before_session_close");

		// Simulate a peer ending the session, as RabbitMQ 4.1 does for an attach to a missing queue.
		this.connectionFactory.getConnection().defaultSession().close();

		await().atMost(Duration.ofSeconds(30))
				.untilAsserted(() -> sendAndReceive(HEALTHY_QUEUE, "after_session_close"));
	}

	private void sendAndReceive(String queue, String body) throws Exception {
		this.amqpClient.to("/queues/" + queue).body(body).send().get(10, TimeUnit.SECONDS);
		BlockingQueue<Message> messages =
				HEALTHY_QUEUE.equals(queue) ? this.testConfig.healthyMessages : this.testConfig.missingMessages;
		Message message = messages.poll(5, TimeUnit.SECONDS);
		assertThat(message)
				.isNotNull()
				.extracting(Message::getBody)
				.isEqualTo(body.getBytes());
	}

	@Configuration(proxyBeanMethods = false)
	@EnableAmqp
	static class TestConfig {

		final BlockingQueue<Message> healthyMessages = new LinkedBlockingQueue<>();

		final BlockingQueue<Message> missingMessages = new LinkedBlockingQueue<>();

		@Bean
		AmqpConnectionFactory amqpConnectionFactory() {
			return new SpringAmqpConnectionFactory()
					.setPort(amqpPort())
					.setReconnectOptions(new ReconnectOptions().reconnectEnabled(true));
		}

		@Bean
		AmqpClient amqpClient(AmqpConnectionFactory connectionFactory) {
			return AmqpClient.create(connectionFactory);
		}

		@Bean
		AmqpMessageListenerContainer healthyContainer(AmqpConnectionFactory connectionFactory) {
			return container(connectionFactory, HEALTHY_QUEUE, this.healthyMessages);
		}

		@Bean
		AmqpMessageListenerContainer missingContainer(AmqpConnectionFactory connectionFactory) {
			return container(connectionFactory, MISSING_QUEUE, this.missingMessages);
		}

		private static AmqpMessageListenerContainer container(AmqpConnectionFactory connectionFactory,
				String queue, BlockingQueue<Message> messages) {

			var listenerContainer = new AmqpMessageListenerContainer(connectionFactory);
			listenerContainer.setQueueNames("/queues/" + queue);
			listenerContainer.setConsumersPerQueue(5);
			listenerContainer.setReceiveTimeout(Duration.ofMillis(100));
			listenerContainer.setRecoveryInterval(Duration.ofMillis(200));
			listenerContainer.setupMessageListener(messages::add);
			return listenerContainer;
		}

	}

}
