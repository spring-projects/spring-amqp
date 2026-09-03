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
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.client.AmqpClient;
import org.springframework.amqp.client.AmqpConnectionFactory;
import org.springframework.amqp.client.SingleAmqpConnectionFactory;
import org.springframework.amqp.client.config.EnableAmqp;
import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the concurrent {@code pause()}/{@code resume()} in the
 * {@link AmqpMessageListenerContainer}.
 *
 * @author Artem Bilan
 *
 * @since 4.1.2
 */
@SpringJUnitConfig
@DirtiesContext
class AmqpMessageListenerContainerPauseTests extends AbstractTestContainerTests {

	static final String PAUSE_QUEUE = "pause_consumer_queue";

	@BeforeAll
	static void initQueue() throws IOException, InterruptedException {
		RABBITMQ.execInContainer("rabbitmqadmin", "queues", "declare", "--name", PAUSE_QUEUE);
	}

	@Autowired
	AmqpClient amqpClient;

	@Autowired
	AmqpMessageListenerContainer container;

	@Test
	void concurrentPauseDoesNotCorruptTheSessionFlow() throws Exception {
		assertThat(this.container.isRunning()).isTrue();

		int threads = 8;
		int rounds = 40;
		Queue<Throwable> errors = new ConcurrentLinkedQueue<>();
		ExecutorService executorService = Executors.newFixedThreadPool(threads);
		CountDownLatch startLatch = new CountDownLatch(1);
		List<Future<?>> futures = new ArrayList<>(threads);

		for (int i = 0; i < threads; i++) {
			futures.add(executorService.submit(() -> {
				try {
					startLatch.await();
					for (int round = 0; round < rounds; round++) {
						this.container.pause();
						this.container.resume();
					}
				}
				catch (Throwable ex) {
					errors.add(ex);
				}
				return null;
			}));
		}

		startLatch.countDown();
		for (Future<?> future : futures) {
			future.get(180, TimeUnit.SECONDS);
		}
		executorService.shutdownNow();

		// Every 'flow' frame for a session is emitted through a single mutable instance
		// shared by all its links, so writing one off the session serializer thread races
		// with the other links and puts an empty 'flow' performative on the wire.
		// The broker rejects that with an internal error and tears down the whole session,
		// including the sender this client uses below.
		this.amqpClient.to("/queues/" + PAUSE_QUEUE)
				.body("after_pause_storm")
				.send();

		assertThat(errors).isEmpty();
	}

	@Configuration(proxyBeanMethods = false)
	@EnableAmqp
	static class TestConfig {

		@Bean
		AmqpConnectionFactory amqpConnectionFactory() {
			return new SingleAmqpConnectionFactory().setPort(amqpPort());
		}

		@Bean
		AmqpClient amqpClient(AmqpConnectionFactory connectionFactory) {
			return AmqpClient.create(connectionFactory);
		}

		@Bean
		AmqpMessageListenerContainer container(AmqpConnectionFactory connectionFactory) {
			var listenerContainer = new AmqpMessageListenerContainer(connectionFactory);
			listenerContainer.setQueueNames("/queues/" + PAUSE_QUEUE);
			listenerContainer.setConsumersPerQueue(8);
			listenerContainer.setInitialCredits(50);
			listenerContainer.setReceiveTimeout(Duration.ofMillis(100));
			listenerContainer.setupMessageListener((message) -> {
			});
			return listenerContainer;
		}

	}

}
