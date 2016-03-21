/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.willDoNothing;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.apache.commons.logging.Log;
import org.junit.Test;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.messaging.handler.annotation.support.MethodArgumentTypeMismatchException;

/**
 * @author Gary Russell
 * @since 1.6
 *
 */
public class ErrorHandlerTests {

	@Test
	public void testFatalsAreRejected() throws Exception {
		ConditionalRejectingErrorHandler handler = new ConditionalRejectingErrorHandler();
		Log logger = spy(TestUtils.getPropertyValue(handler, "logger", Log.class));
		willDoNothing().given(logger).warn(anyString(), any(Throwable.class));
		new DirectFieldAccessor(handler).setPropertyValue("logger", logger);
		handler.handleError(new ListenerExecutionFailedException("intended", new RuntimeException()));

		try {
			handler.handleError(new ListenerExecutionFailedException("intended", new MessageConversionException("")));
			fail("Expected exception");
		}
		catch (AmqpRejectAndDontRequeueException e) {
		}

		try {
			handler.handleError(new ListenerExecutionFailedException("intended",
					new org.springframework.messaging.converter.MessageConversionException("")));
			fail("Expected exception");
		}
		catch (AmqpRejectAndDontRequeueException e) {
		}

		Message<?> message = mock(Message.class);
		MethodParameter mp = new MethodParameter(Foo.class.getMethod("foo", String.class), 0);
		try {
			handler.handleError(new ListenerExecutionFailedException("intended",
					new MethodArgumentNotValidException(message, mp)));
			fail("Expected exception");
		}
		catch (AmqpRejectAndDontRequeueException e) {
		}

		try {
			handler.handleError(new ListenerExecutionFailedException("intended",
					new MethodArgumentTypeMismatchException(message, mp, "")));
			fail("Expected exception");
		}
		catch (AmqpRejectAndDontRequeueException e) {
		}
	}

	private static class Foo {

		@SuppressWarnings("unused")
		public void foo(String foo) {
		}

	}

}
