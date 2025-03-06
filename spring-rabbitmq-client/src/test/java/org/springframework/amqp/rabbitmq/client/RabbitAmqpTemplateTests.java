/*
 * Copyright 2025 the original author or authors.
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

package org.springframework.amqp.rabbitmq.client;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpIllegalStateException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * @author Artem Bilan
 *
 * @since 4.0
 */
@ContextConfiguration
public class RabbitAmqpTemplateTests extends RabbitAmqpTestBase {

	RabbitAmqpTemplate rabbitAmqpTemplate;

	@BeforeEach
	void setUp() {
		this.rabbitAmqpTemplate = new RabbitAmqpTemplate(this.connectionFactory);
	}

	@AfterEach
	void tearDown() {
		this.rabbitAmqpTemplate.destroy();
	}

	@Test
	void illegalStateOnNoDefaults() {
		assertThatIllegalStateException()
				.isThrownBy(() -> this.template.send(new Message(new byte[0])))
				.withMessage(
						"For send with defaults, an 'exchange' (and optional 'routingKey') or 'queue' must be provided");

		assertThatIllegalStateException()
				.isThrownBy(() -> this.template.convertAndSend(new byte[0]))
				.withMessage(
						"For send with defaults, an 'exchange' (and optional 'routingKey') or 'queue' must be provided");
	}

	@Test
	void defaultExchangeAndRoutingKey() {
		this.rabbitAmqpTemplate.setExchange("e1");
		this.rabbitAmqpTemplate.setRoutingKey("k1");

		assertThat(this.rabbitAmqpTemplate.convertAndSend("test1"))
				.succeedsWithin(Duration.ofSeconds(10));

		assertThat(this.rabbitAmqpTemplate.receiveAndConvert("q1"))
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo("test1");
	}

	@Test
	void defaultQueues() {
		this.rabbitAmqpTemplate.setQueue("q1");
		this.rabbitAmqpTemplate.setReceiveQueue("q1");

		assertThat(this.rabbitAmqpTemplate.convertAndSend("test2"))
				.succeedsWithin(Duration.ofSeconds(10));

		assertThat(this.rabbitAmqpTemplate.receiveAndConvert())
				.succeedsWithin(Duration.ofSeconds(10))
				.isEqualTo("test2");
	}

	@Test
	void verifyRpc() {
		String testRequest = "rpc-request";
		String testReply = "rpc-reply";

		CompletableFuture<Object> rpcClientResult = this.template.convertSendAndReceive("e1", "k1", testRequest);

		AtomicReference<String> receivedRequest = new AtomicReference<>();
		CompletableFuture<Boolean> rpcServerResult =
				this.rabbitAmqpTemplate.<String, String>receiveAndReply("q1",
						payload -> {
							receivedRequest.set(payload);
							return testReply;
						});

		assertThat(rpcServerResult).succeedsWithin(Duration.ofSeconds(10)).isEqualTo(true);
		assertThat(rpcClientResult).succeedsWithin(Duration.ofSeconds(10)).isEqualTo(testReply);
		assertThat(receivedRequest.get()).isEqualTo(testRequest);

		this.template.send("q1",
				MessageBuilder.withBody("non-rpc-request".getBytes(StandardCharsets.UTF_8))
						.setMessageId(UUID.randomUUID().toString())
						.setContentType(MimeTypeUtils.TEXT_PLAIN_VALUE)
						.build());

		rpcServerResult = this.rabbitAmqpTemplate.<String, String>receiveAndReply("q1", payload -> "reply-attempt");

		assertThat(rpcServerResult).failsWithin(Duration.ofSeconds(10))
				.withThrowableOfType(ExecutionException.class)
				.withCauseInstanceOf(AmqpIllegalStateException.class)
				.withRootCauseInstanceOf(IllegalArgumentException.class)
				.withMessageContaining("Failed to process RPC request: (Body:'non-rpc-request'")
				.withStackTraceContaining("The 'reply-to' property has to be set on request. Used for reply publishing.");

		rpcClientResult = this.template.convertSendAndReceive("q1", testRequest);
		rpcServerResult = this.rabbitAmqpTemplate.<String, String>receiveAndReply("q1", payload -> null);

		assertThat(rpcServerResult).succeedsWithin(Duration.ofSeconds(10)).isEqualTo(false);
		assertThat(rpcClientResult).failsWithin(Duration.ofSeconds(2))
				.withThrowableThat()
				.isInstanceOf(TimeoutException.class);

		this.template.convertSendAndReceive("q1", new byte[0]);

		rpcServerResult = this.rabbitAmqpTemplate.<String, String>receiveAndReply("q1", payload -> payload);
		assertThat(rpcServerResult).failsWithin(Duration.ofSeconds(10))
				.withThrowableOfType(ExecutionException.class)
				.withCauseInstanceOf(AmqpIllegalStateException.class)
				.withRootCauseInstanceOf(ClassCastException.class)
				.withMessageContaining("Failed to process RPC request: (Body:'[B")
				.withStackTraceContaining("class [B cannot be cast to class java.lang.String");

		assertThat(this.template.receiveAndConvert("dlq1")).succeedsWithin(10, TimeUnit.SECONDS)
				.isEqualTo("non-rpc-request");

		assertThat(this.template.receiveAndConvert("dlq1")).succeedsWithin(10, TimeUnit.SECONDS)
				.isEqualTo(new byte[0]);
	}

	@Configuration
	static class Config {

		@Bean
		DirectExchange e1() {
			return new DirectExchange("e1");
		}

		@Bean
		Queue q1() {
			return QueueBuilder.durable("q1").deadLetterExchange("dlx1").build();
		}

		@Bean
		Binding b1() {
			return BindingBuilder.bind(q1()).to(e1()).with("k1");
		}

	}

}
