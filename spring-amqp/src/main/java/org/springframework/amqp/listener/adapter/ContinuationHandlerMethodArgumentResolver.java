/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.listener.adapter;

import reactor.core.publisher.Mono;

import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;

/**
 * No-op resolver for method arguments of type {@link kotlin.coroutines.Continuation}.
 * <p>
 * This class is similar to
 * {@link org.springframework.messaging.handler.annotation.reactive.ContinuationHandlerMethodArgumentResolver}
 * but for regular {@link HandlerMethodArgumentResolver} contract.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 *
 * @see org.springframework.messaging.handler.annotation.reactive.ContinuationHandlerMethodArgumentResolver
 */
public class ContinuationHandlerMethodArgumentResolver implements HandlerMethodArgumentResolver {

	@Override
	public boolean supportsParameter(MethodParameter parameter) {
		return "kotlin.coroutines.Continuation".equals(parameter.getParameterType().getName());
	}

	@Override
	public Object resolveArgument(MethodParameter parameter, Message<?> message) {
		return Mono.empty();
	}

}
