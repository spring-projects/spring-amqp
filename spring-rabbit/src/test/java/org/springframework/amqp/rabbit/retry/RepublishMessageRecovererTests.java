/*
 * Copyright 2014-2025 the original author or authors.
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

package org.springframework.amqp.rabbit.retry;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

/**
 * @author James Carr
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.3
 */
@ExtendWith(MockitoExtension.class)
public class RepublishMessageRecovererTests {

	private final Message message = new Message("".getBytes(), new MessageProperties());

	private final Throwable cause = new Exception(new Exception("I am Error. When all else fails use fire."));

	@Mock
	private AmqpTemplate amqpTemplate;

	private RepublishMessageRecoverer recoverer;

	@BeforeEach
	void beforeEach() {
		message.getMessageProperties().setReceivedRoutingKey("some.key");
	}

	@Test
	void shouldPublishWithRoutingKeyPrefixedWithErrorWhenExchangeIsNotSet() {
		recoverer = new RepublishMessageRecoverer(amqpTemplate);
		recoverer.recover(message, cause);

		verify(amqpTemplate).send("error.some.key", message);
	}

	@Test
	void nullCauseMessage() {
		recoverer = new RepublishMessageRecoverer(amqpTemplate);
		recoverer.recover(message, new RuntimeException(new RuntimeException()));

		verify(amqpTemplate).send("error.some.key", message);
	}

	@Test
	void shouldPublishWithSetErrorRoutingKeyWhenExchangeAndErrorRoutingKeyProvided() {
		recoverer = new RepublishMessageRecoverer(amqpTemplate, "errorExchange", "errorRoutingKey");
		recoverer.recover(message, cause);

		verify(amqpTemplate).send("errorExchange", "errorRoutingKey", message);
	}

	@Test
	void shouldPublishToProvidedExchange() {
		recoverer = new RepublishMessageRecoverer(amqpTemplate, "error");

		recoverer.recover(message, cause);

		verify(amqpTemplate).send("error", "error.some.key", message);
	}

	@Test
	void shouldIncludeTheStacktraceInTheHeaderOfThePublishedMessage() {
		recoverer = new RepublishMessageRecoverer(amqpTemplate);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		cause.printStackTrace(new PrintStream(baos));
		final String expectedHeaderValue = baos.toString();

		recoverer.recover(message, cause);

		assertThat(message.getMessageProperties().getHeaders().get("x-exception-stacktrace")).isEqualTo(expectedHeaderValue);
	}

	@Test
	void shouldIncludeTheCauseMessageInTheHeaderOfThePublishedMessage() {
		recoverer = new RepublishMessageRecoverer(amqpTemplate);
		recoverer.recover(message, cause);

		assertThat(message.getMessageProperties().getHeaders().get("x-exception-message")).isEqualTo(cause.getCause().getMessage());
	}

	@Test
	void shouldSetTheOriginalMessageExchangeOnInTheHeaders() {
		message.getMessageProperties().setReceivedExchange("the.original.exchange");
		recoverer = new RepublishMessageRecoverer(amqpTemplate, "error");

		recoverer.recover(message, cause);

		assertThat(message.getMessageProperties().getHeaders().get("x-original-exchange")).isEqualTo("the.original.exchange");
	}

	@Test
	void shouldRemapDeliveryMode() {
		message.getMessageProperties().setDeliveryMode(null);
		message.getMessageProperties().setReceivedDeliveryMode(MessageDeliveryMode.PERSISTENT);
		recoverer = new RepublishMessageRecoverer(amqpTemplate, "error") {

			@Override
			protected Map<? extends String, ? extends Object> additionalHeaders(Message message, Throwable cause) {
				message.getMessageProperties().setDeliveryMode(message.getMessageProperties().getReceivedDeliveryMode());
				return null;
			}

		};

		recoverer.recover(message, cause);

		assertThat(message.getMessageProperties().getDeliveryMode()).isEqualTo(MessageDeliveryMode.PERSISTENT);
	}

	@Test
	void setDeliveryModeIfNull() {
		this.message.getMessageProperties().setDeliveryMode(null);
		this.recoverer = new RepublishMessageRecoverer(amqpTemplate, "error");

		this.recoverer.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
		recoverer.recover(this.message, this.cause);

		assertThat(this.message.getMessageProperties().getDeliveryMode()).isEqualTo(MessageDeliveryMode.NON_PERSISTENT);
	}

	@Test
	void dynamicExRk() {
		this.recoverer = new RepublishMessageRecoverer(this.amqpTemplate,
				new SpelExpressionParser().parseExpression("messageProperties.headers.get('errorExchange')"),
				new SpelExpressionParser().parseExpression("messageProperties.headers.get('errorRK')"));
		this.message.getMessageProperties().setHeader("errorExchange", "ex");
		this.message.getMessageProperties().setHeader("errorRK", "rk");

		this.recoverer.recover(this.message, this.cause);

		verify(this.amqpTemplate).send("ex", "rk", this.message);
	}

	@Test
	void dynamicRk() {
		this.recoverer = new RepublishMessageRecoverer(this.amqpTemplate, null,
				new SpelExpressionParser().parseExpression("messageProperties.headers.get('errorRK')"));
		this.message.getMessageProperties().setHeader("errorExchange", "ex");
		this.message.getMessageProperties().setHeader("errorRK", "rk");

		this.recoverer.recover(this.message, this.cause);

		verify(this.amqpTemplate).send("rk", this.message);
	}

}
