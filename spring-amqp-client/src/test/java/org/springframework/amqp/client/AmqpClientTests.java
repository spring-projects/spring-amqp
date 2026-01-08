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

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.json.JsonMapper;

import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
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
	AmqpClient amqpClient;

	@Test
	void sendNativeWithDefaultTo() throws JsonProcessingException {
		TestData testData = new TestData("test_data");
		byte[] testDataBytes = JsonMapper.builder().build().writeValueAsBytes(testData);

		CompletableFuture<Boolean> sendFuture = this.amqpClient.send(Message.create(testDataBytes));

		assertThat(sendFuture)
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo(Boolean.TRUE);

		CompletableFuture<TestData> receiveFuture =
				this.amqpClient.from("/queues/" + TEST_SEND_QUEUE)
						.receiveAndConvert();

		assertThat(receiveFuture)
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo(testData);

		this.amqpClient.send(Message.create("\"more_data\"".getBytes()));

		assertThat(this.amqpClient.from("/queues/" + TEST_SEND_QUEUE).receiveAndConvert())
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo("more_data");

	}

	@Test
	void sendToWithMessage() {
		CompletableFuture<Boolean> sendFuture =
				this.amqpClient
						.to("/queues/" + TEST_SEND_QUEUE2)
						.message(new org.springframework.amqp.core.Message("\"test_data2\"".getBytes()))
						.send();

		assertThat(sendFuture)
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo(Boolean.TRUE);

		CompletableFuture<String> receiveFuture =
				this.amqpClient.from("/queues/" + TEST_SEND_QUEUE2)
						.receiveAndConvert();

		assertThat(receiveFuture)
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo("test_data2");
	}

	@Test
	void convertAndSend() {
		CompletableFuture<Boolean> sendFuture =
				this.amqpClient
						.to("/queues/" + TEST_SEND_QUEUE2)
						.body("convert")
						.priority(7)
						.header("test_header", "test_value")
						.messageId("some_id")
						.userId("guest")
						.durable(false)
						.send();

		assertThat(sendFuture)
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo(Boolean.TRUE);

		CompletableFuture<org.springframework.amqp.core.Message> receiveFuture =
				this.amqpClient.from("/queues/" + TEST_SEND_QUEUE2)
						.timeout(Duration.ofSeconds(1))
						.receive();

		assertThat(receiveFuture)
				.succeedsWithin(Duration.ofSeconds(10))
				.satisfies(message -> {
					assertThat(message.getBody()).isEqualTo("\"convert\"".getBytes());
					assertThat(message.getMessageProperties())
							.satisfies(props -> {
								assertThat(props.getPriority()).isEqualTo(7);
								assertThat(props.getUserId()).isEqualTo("guest");
								assertThat(props.getMessageId()).isEqualTo("some_id");
								assertThat(props.getDeliveryMode()).isEqualTo(MessageDeliveryMode.NON_PERSISTENT);
								assertThat(props.getHeaders()).containsEntry("test_header", "test_value");
							});
				});
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
					.messageConverter(new JacksonJsonMessageConverter())
					.build();
		}

	}

	private record TestData(String data) {

	}

}
