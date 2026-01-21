/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import org.apache.commons.logging.Log;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.listener.ConditionalRejectingErrorHandler;
import org.springframework.amqp.listener.ListenerExecutionFailedException;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.messaging.handler.annotation.support.MethodArgumentTypeMismatchException;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.willDoNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Ngoc Nhan
 *
 * @since 1.6
 *
 */
public class ErrorHandlerTests {

	@Test
	public void testFatalsAreRejected() throws Exception {
		ConditionalRejectingErrorHandler handler = new ConditionalRejectingErrorHandler();
		Log logger = spy(TestUtils.<Log>propertyValue(handler, "logger"));
		willDoNothing().given(logger).warn(anyString(), any(Throwable.class));
		new DirectFieldAccessor(handler).setPropertyValue("logger", logger);
		handler.handleError(new ListenerExecutionFailedException("intended", new RuntimeException(),
				new org.springframework.amqp.core.Message("".getBytes(), new MessageProperties())));

		assertThatExceptionOfType(AmqpRejectAndDontRequeueException.class)
				.isThrownBy(() -> handler.handleError(new ListenerExecutionFailedException("intended",
						new MessageConversionException(""),
						new org.springframework.amqp.core.Message("".getBytes(), new MessageProperties()))));

		assertThatExceptionOfType(AmqpRejectAndDontRequeueException.class)
				.isThrownBy(() -> handler.handleError(new ListenerExecutionFailedException("intended",
						new org.springframework.messaging.converter.MessageConversionException(""),
						new org.springframework.amqp.core.Message("".getBytes(), new MessageProperties()))));

		Message<?> message = mock(Message.class);
		MethodParameter mp = new MethodParameter(Foo.class.getMethod("foo", String.class), 0);
		assertThatExceptionOfType(AmqpRejectAndDontRequeueException.class)
				.isThrownBy(() -> handler.handleError(new ListenerExecutionFailedException("intended",
						new MethodArgumentNotValidException(message, mp),
						new org.springframework.amqp.core.Message("".getBytes(), new MessageProperties()))));

		assertThatExceptionOfType(AmqpRejectAndDontRequeueException.class)
				.isThrownBy(() -> handler.handleError(new ListenerExecutionFailedException("intended",
						new MethodArgumentTypeMismatchException(message, mp, ""),
						new org.springframework.amqp.core.Message("".getBytes(), new MessageProperties()))));
	}

	@Test
	public void testSimple() {
		Throwable cause = new ClassCastException();
		assertThatExceptionOfType(AmqpRejectAndDontRequeueException.class).isThrownBy(() -> doTest(cause));
	}

	@Test
	public void testMessagingException() {
		Throwable cause = new MessageHandlingException(null, "test",
				new MessageHandlingException(null, "test", new ClassCastException()));
		assertThatExceptionOfType(AmqpRejectAndDontRequeueException.class).isThrownBy(() -> doTest(cause));
	}

	private void doTest(Throwable cause) {
		ConditionalRejectingErrorHandler handler = new ConditionalRejectingErrorHandler();
		handler.handleError(
				new ListenerExecutionFailedException("test", cause,
						new org.springframework.amqp.core.Message(new byte[0],
								new MessageProperties())));
	}

	private static final class Foo {

		@SuppressWarnings("unused")
		public void foo(String foo) {
		}

	}

}
