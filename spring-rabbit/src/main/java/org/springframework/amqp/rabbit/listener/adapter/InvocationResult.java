/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.adapter;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

import org.jspecify.annotations.Nullable;

import org.springframework.expression.Expression;

/**
 * The result of a listener method invocation.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.1
 *
 * @deprecated since 4.1 in favor of {@link org.springframework.amqp.listener.adapter.InvocationResult}.
 */
@Deprecated(since = "4.1", forRemoval = true)
public final class InvocationResult extends org.springframework.amqp.listener.adapter.InvocationResult {

	/**
	 * Construct an instance with the provided properties.
	 * @param result the result.
	 * @param sendTo the sendTo expression.
	 * @param returnType the return type.
	 * @param bean the bean.
	 * @param method the method.
	 */
	public InvocationResult(@Nullable Object result, @Nullable Expression sendTo, @Nullable Type returnType,
			@Nullable Object bean, @Nullable Method method) {

		super(result, sendTo, returnType, bean, method);
	}

}
