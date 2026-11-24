/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp.rabbit.listener.adapter;

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
 * @since 3.0.5
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
