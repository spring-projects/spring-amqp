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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.client.AmqpClient;
import org.springframework.amqp.client.AmqpConnectionFactory;
import org.springframework.amqp.client.SingleAmqpConnectionFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Artem Bilan
 *
 * @since 4.1
 */
@SpringJUnitConfig
@DirtiesContext
public class AmqpMessageListenerContainerTests extends AbstractTestContainerTests {

	static final String TEST_QUEUE1 = "/queues/test_queue1";

	static final String TEST_QUEUE2 = "/queues/test_queue2";

	static final String[] QUEUE_NAMES = {
			TEST_QUEUE1,
			TEST_QUEUE2
	};

	@BeforeAll
	static void initQueues() throws IOException, InterruptedException {
		for (String queue : QUEUE_NAMES) {
			RABBITMQ.execInContainer("rabbitmqadmin", "queues", "declare", "--name", queue.replaceFirst("/queues/", ""));
		}
	}

	@Autowired
	AmqpClient amqpClient;

	@Autowired
	AmqpMessageListenerContainer amqpMessageListenerContainer;

	@Autowired
	TestConfig testConfig;

	@Test
	void messagesConsumedFromAllQueues() throws InterruptedException {
		Map<String, String> dataToQueue =
				Map.of(
						"test_data1", TEST_QUEUE1,
						"test_data2", TEST_QUEUE2,
						"test_data3", TEST_QUEUE1,
						"test_data4", TEST_QUEUE2);

		for (Map.Entry<String, String> entry : dataToQueue.entrySet()) {
			assertThat(this.amqpClient.to(entry.getValue()).body(entry.getKey()).send())
					.succeedsWithin(Duration.ofSeconds(10));
		}

		for (int i = 0; i < 4; i++) {
			Message message = this.testConfig.receivedMessages.poll(10, TimeUnit.SECONDS);
			assertThat(message)
					.isNotNull()
					.satisfies(m ->
							assertThat(dataToQueue)
									.containsEntry(new String(m.getBody()), m.getMessageProperties().getReceivedRoutingKey()));
			message.getMessageProperties().getAmqpAcknowledgment().acknowledge();
		}
		Message noMessage = this.testConfig.receivedMessages.poll(1, TimeUnit.SECONDS);
		assertThat(noMessage).isNull();
	}

	@Test
	void pauseAndResumeContainer() throws InterruptedException {
		this.amqpMessageListenerContainer.pause();

		assertThat(this.amqpClient.to(TEST_QUEUE1).body("after resume").send())
				.succeedsWithin(Duration.ofSeconds(10));

		Message message = this.testConfig.receivedMessages.poll(1, TimeUnit.SECONDS);
		assertThat(message).isNull();

		this.amqpMessageListenerContainer.resume();

		message = this.testConfig.receivedMessages.poll(1, TimeUnit.SECONDS);
		assertThat(message)
				.extracting(Message::getBody)
				.isEqualTo("after resume".getBytes());
	}

	@Configuration(proxyBeanMethods = false)
	static class TestConfig {

		BlockingQueue<Message> receivedMessages = new LinkedBlockingQueue<>();

		@Bean
		Client protonClient() {
			return Client.create();
		}

		@Bean
		AmqpConnectionFactory amqpConnectionFactory(Client protonClient) {
			return new SingleAmqpConnectionFactory(protonClient)
					.setPort(amqpPort());
		}

		@Bean
		AmqpClient amqpClient(AmqpConnectionFactory connectionFactory) {
			return AmqpClient.create(connectionFactory);
		}

		@Bean
		AmqpMessageListenerContainer amqpMessageListenerContainer(AmqpConnectionFactory connectionFactory) {
			var amqpMessageListenerContainer = new AmqpMessageListenerContainer(connectionFactory);
			amqpMessageListenerContainer.setQueueNames(QUEUE_NAMES);
			amqpMessageListenerContainer.setConsumersPerQueue(3);
			amqpMessageListenerContainer.setAutoAccept(false);
			amqpMessageListenerContainer.setReceiveTimeout(Duration.ofMillis(100));
			amqpMessageListenerContainer.setupMessageListener(this.receivedMessages::add);
			return amqpMessageListenerContainer;
		}

	}

}
