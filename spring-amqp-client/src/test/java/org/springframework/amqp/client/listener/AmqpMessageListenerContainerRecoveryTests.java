/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.client.listener;

import java.io.IOException;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.ReconnectOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.client.AmqpClient;
import org.springframework.amqp.client.AmqpConnectionFactory;
import org.springframework.amqp.client.SingleAmqpConnectionFactory;
import org.springframework.amqp.client.config.EnableAmqp;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests for a broken receiver link recovery in the {@link AmqpMessageListenerContainer}.
 *
 * @author Artem Bilan
 *
 * @since 4.1.1
 */
@SpringJUnitConfig
@DirtiesContext
class AmqpMessageListenerContainerRecoveryTests extends AbstractTestContainerTests {

	static final String RECOVERABLE_QUEUE = "recoverable_consumer_queue";

	static final String UNRECOVERABLE_QUEUE = "unrecoverable_consumer_queue";

	@BeforeAll
	static void initQueues() throws IOException, InterruptedException {
		declareQueue(RECOVERABLE_QUEUE);
		declareQueue(UNRECOVERABLE_QUEUE);
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
	@Qualifier("recoverableContainer")
	AmqpMessageListenerContainer recoverableContainer;

	@Autowired
	@Qualifier("unrecoverableContainer")
	AmqpMessageListenerContainer unrecoverableContainer;

	@Test
	void consumerRecoversItsReceiverWhenTheLinkIsDetached() throws IOException, InterruptedException {
		assertThat(this.recoverableContainer.isRunning()).isTrue();

		// Deleting the queue detaches the receiver link, and the ProtonJ client does not restore it.
		deleteQueue(RECOVERABLE_QUEUE);

		// The failing 'receive()' must be reported, but only a handful of times,
		// not millions of times per second like a tight retry loop would.
		await().atMost(Duration.ofSeconds(30)).until(() -> !this.testConfig.recoverableErrors.isEmpty());

		declareQueue(RECOVERABLE_QUEUE);

		// The consumer re-opens its receiver, so the messages are delivered again.
		await().atMost(Duration.ofSeconds(60))
				.untilAsserted(() -> {
					this.amqpClient.to("/queues/" + RECOVERABLE_QUEUE).body("after_recovery").send();
					Message message = this.testConfig.receivedMessages.poll(2, TimeUnit.SECONDS);
					assertThat(message)
							.isNotNull()
							.extracting(Message::getBody)
							.isEqualTo("after_recovery".getBytes());
				});

		assertThat(this.recoverableContainer.isRunning()).isTrue();
		assertThat(this.testConfig.recoverableErrors).hasSizeLessThan(100);
	}

	@Test
	void containerIsNotRunningWhenConsumerGivesUpRecovery() throws IOException, InterruptedException {
		assertThat(this.unrecoverableContainer.isRunning()).isTrue();

		// The queue is gone for good, so re-opening the receiver keeps failing
		// until the 'recoveryBackOff' returns 'BackOffExecution.STOP'.
		deleteQueue(UNRECOVERABLE_QUEUE);

		// The only consumer removes itself, so the container must stop reporting itself as running.
		await().atMost(Duration.ofSeconds(30)).until(() -> !this.unrecoverableContainer.isRunning());

		assertThat(this.testConfig.unrecoverableErrors).hasSizeLessThan(100);
	}

	@Configuration(proxyBeanMethods = false)
	@EnableAmqp
	static class TestConfig {

		final BlockingQueue<Message> receivedMessages = new LinkedBlockingQueue<>();

		final Queue<Throwable> recoverableErrors = new ConcurrentLinkedQueue<>();

		final Queue<Throwable> unrecoverableErrors = new ConcurrentLinkedQueue<>();

		@Bean
		AmqpConnectionFactory amqpConnectionFactory() {
			return new SingleAmqpConnectionFactory()
					.setPort(amqpPort())
					.setReconnectOptions(new ReconnectOptions().reconnectEnabled(true));
		}

		@Bean
		AmqpClient amqpClient(AmqpConnectionFactory connectionFactory) {
			return AmqpClient.create(connectionFactory);
		}

		@Bean
		AmqpMessageListenerContainer recoverableContainer(AmqpConnectionFactory connectionFactory) {
			var listenerContainer = new AmqpMessageListenerContainer(connectionFactory);
			listenerContainer.setQueueNames("/queues/" + RECOVERABLE_QUEUE);
			listenerContainer.setReceiveTimeout(Duration.ofMillis(100));
			listenerContainer.setRecoveryInterval(Duration.ofMillis(200));
			listenerContainer.setErrorHandler(this.recoverableErrors::add);
			listenerContainer.setupMessageListener(this.receivedMessages::add);
			return listenerContainer;
		}

		@Bean
		AmqpMessageListenerContainer unrecoverableContainer(AmqpConnectionFactory connectionFactory) {
			var listenerContainer = new AmqpMessageListenerContainer(connectionFactory);
			listenerContainer.setQueueNames("/queues/" + UNRECOVERABLE_QUEUE);
			listenerContainer.setReceiveTimeout(Duration.ofMillis(100));
			listenerContainer.setRecoveryBackOff(new FixedBackOff(200, 2));
			listenerContainer.setErrorHandler(this.unrecoverableErrors::add);
			listenerContainer.setupMessageListener(this.receivedMessages::add);
			return listenerContainer;
		}

	}

}
