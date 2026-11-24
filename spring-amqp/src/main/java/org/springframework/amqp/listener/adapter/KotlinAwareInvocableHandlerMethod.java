/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp.listener.adapter;

import java.lang.reflect.Method;

import org.jspecify.annotations.Nullable;

import org.springframework.core.CoroutinesUtils;
import org.springframework.core.KotlinDetector;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * An {@link InvocableHandlerMethod} extension for supporting Kotlin {@code suspend} function.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class KotlinAwareInvocableHandlerMethod extends InvocableHandlerMethod {

	public KotlinAwareInvocableHandlerMethod(Object bean, Method method) {
		super(bean, method);
	}

	@Override
	protected @Nullable Object doInvoke(@Nullable Object... args) throws Exception {
		Method method = getBridgedMethod();
		if (KotlinDetector.isSuspendingFunction(method)) {
			return CoroutinesUtils.invokeSuspendingFunction(method, getBean(), args);
		}
		else {
			return super.doInvoke(args);
		}
	}

}
