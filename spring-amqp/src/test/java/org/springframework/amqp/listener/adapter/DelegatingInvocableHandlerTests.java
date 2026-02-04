/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.amqp.listener.adapter;

import java.lang.reflect.Method;
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

import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 4.1
 *
 */
public class DelegatingInvocableHandlerTests {

	@Test
	void multiNoMatch() throws Exception {
		List<InvocableHandlerMethod> methods = new ArrayList<>();
		Object bean = new Multi();
		Method method = Multi.class.getDeclaredMethod("listen", Integer.class);
		methods.add(messageHandlerFactory().createInvocableHandlerMethod(bean, method));
		BeanExpressionResolver resolver = mock();
		BeanExpressionContext context = mock();
		DelegatingInvocableHandler handler = new DelegatingInvocableHandler(methods, bean, resolver, context);
		assertThatIllegalStateException()
				.isThrownBy(() -> handler.getHandlerForPayload(Long.class))
				.withCauseExactlyInstanceOf(NoSuchMethodException.class)
				.withStackTraceContaining("No listener method found in");
	}

	private static MessageHandlerMethodFactory messageHandlerFactory() {
		DefaultMessageHandlerMethodFactory defaultFactory = new DefaultMessageHandlerMethodFactory();
		DefaultFormattingConversionService cs = new DefaultFormattingConversionService();
		defaultFactory.setConversionService(cs);
		GenericMessageConverter messageConverter = new GenericMessageConverter(cs);
		defaultFactory.setMessageConverter(messageConverter);
		defaultFactory.afterPropertiesSet();
		return defaultFactory;
	}

	static class Multi {

		void listen(Integer in) {
		}

	}

}
