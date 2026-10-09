/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.adapter;

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
		assertThatIllegalStateException()
				.isThrownBy(() ->
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
