/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.adapter;

import java.lang.reflect.Method;

import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * An {@link InvocableHandlerMethod} extension for supporting Kotlin {@code suspend} function.
 *
 * @author Artem Bilan
 *
 * @since 3.0.5
 *
 * @deprecated since 4.1 in favor of Spring AMQP's {@link org.springframework.amqp.listener.adapter.KotlinAwareInvocableHandlerMethod}.
 */
@Deprecated(forRemoval = true, since = "4.1")
public class KotlinAwareInvocableHandlerMethod extends org.springframework.amqp.listener.adapter.KotlinAwareInvocableHandlerMethod {

	public KotlinAwareInvocableHandlerMethod(Object bean, Method method) {
		super(bean, method);
	}

}
