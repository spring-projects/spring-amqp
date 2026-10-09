/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.adapter;

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
 *
 * @deprecated since 4.1 in favor of Spring AMQP's {@link org.springframework.amqp.listener.adapter.ContinuationHandlerMethodArgumentResolver}.
 */
@Deprecated(forRemoval = true, since = "4.1")
public class ContinuationHandlerMethodArgumentResolver extends org.springframework.amqp.listener.adapter.ContinuationHandlerMethodArgumentResolver {

}
