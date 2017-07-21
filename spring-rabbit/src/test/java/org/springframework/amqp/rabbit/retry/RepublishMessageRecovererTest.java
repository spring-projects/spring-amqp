/*
 * Copyright 2014-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.retry;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;

/**
 * @author James Carr
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.3
 */
@RunWith(MockitoJUnitRunner.class)
public class RepublishMessageRecovererTest {

	private final Message message = new Message("".getBytes(), new MessageProperties());

	private final Throwable cause = new Exception(new Exception("I am Error. When all else fails use fire."));

	@Mock
	private AmqpTemplate amqpTemplate;

	private RepublishMessageRecoverer recoverer;

	@Before
	public void beforeEach() {
		message.getMessageProperties().setReceivedRoutingKey("some.key");
	}

	@Test
	public void shouldPublishWithRoutingKeyPrefixedWithErrorWhenExchangeIsNotSet() {
		recoverer = new RepublishMessageRecoverer(amqpTemplate);
		recoverer.recover(message, cause);

		verify(amqpTemplate).send("error.some.key", message);
	}

	@Test
	public void shouldPublishWithSetErrorRoutingKeyWhenExchangeAndErrorRoutingKeyProvided() {
		recoverer = new RepublishMessageRecoverer(amqpTemplate, "errorExchange", "errorRoutingKey");
		recoverer.recover(message, cause);

		verify(amqpTemplate).send("errorExchange", "errorRoutingKey", message);
	}

	@Test
	public void shouldPublishToProvidedExchange() {
		recoverer = new RepublishMessageRecoverer(amqpTemplate, "error");

		recoverer.recover(message, cause);

		verify(amqpTemplate).send("error", "error.some.key", message);
	}

	@Test
	public void shouldIncludeTheStacktraceInTheHeaderOfThePublishedMessage() {
		recoverer = new RepublishMessageRecoverer(amqpTemplate);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		cause.printStackTrace(new PrintStream(baos));
		final String expectedHeaderValue = baos.toString();

		recoverer.recover(message, cause);

		assertEquals(expectedHeaderValue, message.getMessageProperties().getHeaders().get("x-exception-stacktrace"));
	}

	@Test
	public void shouldIncludeTheCauseMessageInTheHeaderOfThePublishedMessage() {
		recoverer = new RepublishMessageRecoverer(amqpTemplate);
		recoverer.recover(message, cause);

		assertEquals(cause.getCause().getMessage(),
				message.getMessageProperties().getHeaders().get("x-exception-message"));
	}

	@Test
	public void shouldSetTheOriginalMessageExchangeOnInTheHeaders() {
		message.getMessageProperties().setReceivedExchange("the.original.exchange");
		recoverer = new RepublishMessageRecoverer(amqpTemplate, "error");

		recoverer.recover(message, cause);

		assertEquals("the.original.exchange",
				message.getMessageProperties().getHeaders().get("x-original-exchange"));
	}

	@Test
	public void shouldRemapDeliveryMode() {
		message.getMessageProperties().setDeliveryMode(null);
		message.getMessageProperties().setReceivedDeliveryMode(MessageDeliveryMode.PERSISTENT);
		recoverer = new RepublishMessageRecoverer(amqpTemplate, "error") {

			protected Map<? extends String, ? extends Object> additionalHeaders(Message message, Throwable cause) {
				message.getMessageProperties().setDeliveryMode(message.getMessageProperties().getReceivedDeliveryMode());
				return null;
			}

		};

		recoverer.recover(message, cause);

		assertEquals(MessageDeliveryMode.PERSISTENT, message.getMessageProperties().getDeliveryMode());
	}

	@Test
	public void setDeliveryModeIfNull() {
		this.message.getMessageProperties().setDeliveryMode(null);
		this.recoverer = new RepublishMessageRecoverer(amqpTemplate, "error");

		this.recoverer.setDeliveryMode(MessageDeliveryMode.NON_PERSISTENT);
		recoverer.recover(this.message, this.cause);

		assertEquals(MessageDeliveryMode.NON_PERSISTENT, this.message.getMessageProperties().getDeliveryMode());
	}

}
