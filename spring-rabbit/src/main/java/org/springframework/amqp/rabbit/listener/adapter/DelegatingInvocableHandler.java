/*
 * Copyright 2015-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.adapter;

import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.validation.Validator;

/**
 * Delegates to an {@link InvocableHandlerMethod} based on the message payload type.
 * Matches a single, non-annotated parameter or one that is annotated with
 * {@link org.springframework.messaging.handler.annotation.Payload}. Matches must be
 * unambiguous.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.5
 *
 * @deprecated since 2.2 in favor of {@link MessagingMessageListenerAdapter}.
 */
@Deprecated(since = "4.1", forRemoval = true)
public class DelegatingInvocableHandler extends org.springframework.amqp.listener.adapter.DelegatingInvocableHandler {

	/**
	 * Construct an instance with the supplied handlers for the bean.
	 * @param handlers the handlers.
	 * @param bean the bean.
	 * @param beanExpressionResolver the resolver.
	 * @param beanExpressionContext the context.
	 */
	public DelegatingInvocableHandler(List<InvocableHandlerMethod> handlers, Object bean,
			BeanExpressionResolver beanExpressionResolver, BeanExpressionContext beanExpressionContext) {

		this(handlers, null, bean, beanExpressionResolver, beanExpressionContext, null);
	}

	/**
	 * Construct an instance with the supplied handlers for the bean.
	 * @param handlers the handlers.
	 * @param defaultHandler the default handler.
	 * @param bean the bean.
	 * @param beanExpressionResolver the resolver.
	 * @param beanExpressionContext the context.
	 * @since 2.0.3
	 */
	public DelegatingInvocableHandler(List<InvocableHandlerMethod> handlers,
			@Nullable InvocableHandlerMethod defaultHandler, Object bean, BeanExpressionResolver beanExpressionResolver,
			BeanExpressionContext beanExpressionContext) {

		this(handlers, defaultHandler, bean, beanExpressionResolver, beanExpressionContext, null);
	}

	/**
	 * Construct an instance with the supplied handlers for the bean.
	 * @param handlers the handlers.
	 * @param defaultHandler the default handler.
	 * @param bean the bean.
	 * @param beanExpressionResolver the resolver.
	 * @param beanExpressionContext the context.
	 * @param validator the validator.
	 * @since 2.0.3
	 */
	public DelegatingInvocableHandler(List<InvocableHandlerMethod> handlers,
			@Nullable InvocableHandlerMethod defaultHandler, Object bean,
			@Nullable BeanExpressionResolver beanExpressionResolver,
			@Nullable BeanExpressionContext beanExpressionContext, @Nullable Validator validator) {

		super(handlers, defaultHandler, bean, beanExpressionResolver, beanExpressionContext, validator);
	}

}
