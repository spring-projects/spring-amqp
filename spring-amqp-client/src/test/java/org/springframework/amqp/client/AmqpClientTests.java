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

package org.springframework.amqp.client;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
public class AmqpClientTests extends AbstractTestContainerTests {

	static final String TEST_SEND_QUEUE = "test_send_queue";

	static final String TEST_SEND_QUEUE2 = "test_send_queue2";

	static final String[] QUEUE_NAMES = {
			TEST_SEND_QUEUE,
			TEST_SEND_QUEUE2
	};

	@BeforeAll
	static void initQueues() throws IOException, InterruptedException {
		for (String queue : QUEUE_NAMES) {
			RABBITMQ.execInContainer("rabbitmqadmin", "queues", "declare", "--name", queue);
		}
	}

	@Autowired
	AmqpConnectionFactory amqpConnectionFactory;

	@Autowired
	AmqpClient amqpClient;

	@Test
	void sendNativeWithDefaultTo() throws ClientException {
		CompletableFuture<Boolean> sendFuture = this.amqpClient.send(Message.create("test_data"));

		assertThat(sendFuture)
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo(Boolean.TRUE);

		Connection connection = this.amqpConnectionFactory.getConnection();
		try (Receiver receiver = connection.openReceiver("/queues/" + TEST_SEND_QUEUE)) {
			Message<String> receivedMessage = receiver.receive(10, TimeUnit.SECONDS).message();
			assertThat(receivedMessage.body()).isEqualTo("test_data");
		}
	}

	@Test
	void sendToWithMessage() throws ClientException {
		CompletableFuture<Boolean> sendFuture =
				this.amqpClient
						.to("/queues/" + TEST_SEND_QUEUE2)
						.message(new org.springframework.amqp.core.Message("test_data2".getBytes()))
						.send();

		assertThat(sendFuture)
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo(Boolean.TRUE);

		Connection connection = this.amqpConnectionFactory.getConnection();
		try (Receiver receiver = connection.openReceiver("/queues/" + TEST_SEND_QUEUE2)) {
			Message<?> receivedMessage = receiver.receive(10, TimeUnit.SECONDS).message();
			assertThat(receivedMessage.body()).isEqualTo("test_data2".getBytes());
		}
	}

	@Test
	void convertAndSend() throws ClientException {
		CompletableFuture<Boolean> sendFuture =
				this.amqpClient
						.to("/queues/" + TEST_SEND_QUEUE2)
						.body("convert")
						.priority(7)
						.header("test_header", "test_value")
						.messageId("some_id")
						.userId("guest")
						.send();

		assertThat(sendFuture)
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo(Boolean.TRUE);

		Connection connection = this.amqpConnectionFactory.getConnection();
		try (Receiver receiver = connection.openReceiver("/queues/" + TEST_SEND_QUEUE2)) {
			Message<?> receivedMessage = receiver.receive(10, TimeUnit.SECONDS).message();
			assertThat(receivedMessage.body()).isEqualTo("convert".getBytes());
			assertThat(receivedMessage.userId()).isEqualTo("guest".getBytes());
			assertThat(receivedMessage.messageId()).isEqualTo("some_id");
			assertThat(receivedMessage.priority()).isEqualTo((byte) 7);
			assertThat(receivedMessage.property("test_header")).isEqualTo("test_value");
		}
	}

	@Test
	void failOnNoAddress() {
		CompletableFuture<Boolean> sendFuture =
				this.amqpClient
						.to("/queues/no_such_queue_" + UUID.randomUUID())
						.body(new byte[0])
						.send();

		assertThat(sendFuture)
				.failsWithin(Duration.ofSeconds(10))
				.withThrowableOfType(ExecutionException.class)
				.withMessage(
						"org.springframework.amqp.client.AmqpClientNackReceivedException: " +
								"The message was not accepted, but " + DeliveryState.Type.RELEASED);
	}

	@Configuration(proxyBeanMethods = false)
	static class TestConfig {

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
			return AmqpClient.builder(connectionFactory)
					.defaultToAddress("/queues/" + TEST_SEND_QUEUE)
					.build();
		}

	}

}
