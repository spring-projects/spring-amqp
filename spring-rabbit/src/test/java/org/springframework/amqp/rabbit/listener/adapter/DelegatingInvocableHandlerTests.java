/*
 * Copyright 2023-2025 the original author or authors.
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

package org.springframework.amqp.rabbit.listener.adapter;

import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 2.4.12
 *
 */
public class DelegatingInvocableHandlerTests {

	@Test
	void multiNoMatch() throws Exception {
		List<InvocableHandlerMethod> methods = new ArrayList<>();
		Object bean = new Multi();
		Method method = Multi.class.getDeclaredMethod("listen", Integer.class);
		methods.add(messageHandlerFactory().createInvocableHandlerMethod(bean, method));
		BeanExpressionResolver resolver = mock(BeanExpressionResolver.class);
		BeanExpressionContext context = mock(BeanExpressionContext.class);
		DelegatingInvocableHandler handler = new DelegatingInvocableHandler(methods, bean, resolver, context);
		assertThatExceptionOfType(UndeclaredThrowableException.class).isThrownBy(() ->
				handler.getHandlerForPayload(Long.class))
				.withCauseExactlyInstanceOf(NoSuchMethodException.class)
				.withStackTraceContaining("No listener method found in");
	}

	private MessageHandlerMethodFactory messageHandlerFactory() {
		DefaultMessageHandlerMethodFactory defaultFactory = new DefaultMessageHandlerMethodFactory();
		DefaultFormattingConversionService cs = new DefaultFormattingConversionService();
		defaultFactory.setConversionService(cs);
		GenericMessageConverter messageConverter = new GenericMessageConverter(cs);
		defaultFactory.setMessageConverter(messageConverter);
		defaultFactory.afterPropertiesSet();
		return defaultFactory;
	}

	public static class Multi {

		void listen(Integer in) {
		}

	}

}
