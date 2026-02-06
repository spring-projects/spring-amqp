/*
 * Copyright 2026-present the original author or authors.
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
import java.lang.reflect.Type;
import java.util.concurrent.CompletableFuture;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.utils.JavaUtils;
import org.springframework.amqp.utils.MonoHandler;
import org.springframework.core.KotlinDetector;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;

/**
 * A wrapper for either an {@link InvocableHandlerMethod} or
 * {@link DelegatingInvocableHandler}. All methods delegate to the
 * underlying handler.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 4.1
 *
 */
public class HandlerAdapter {

	private final @Nullable InvocableHandlerMethod invokerHandlerMethod;

	private final @Nullable DelegatingInvocableHandler delegatingHandler;

	private final boolean asyncReplies;

	/**
	 * Construct an instance with the provided method.
	 * @param invokerHandlerMethod the method.
	 */
	public HandlerAdapter(InvocableHandlerMethod invokerHandlerMethod) {
		this.invokerHandlerMethod = invokerHandlerMethod;
		this.delegatingHandler = null;
		Method method = invokerHandlerMethod.getMethod();
		Class<?> returnType = method.getReturnType();
		this.asyncReplies = (JavaUtils.MONO_PRESENT && MonoHandler.isMono(returnType))
				|| CompletableFuture.class.isAssignableFrom(returnType)
				|| KotlinDetector.isSuspendingFunction(method);
	}

	/**
	 * Construct an instance with the provided delegating handler.
	 * @param delegatingHandler the handler.
	 */
	public HandlerAdapter(DelegatingInvocableHandler delegatingHandler) {
		this.invokerHandlerMethod = null;
		this.delegatingHandler = delegatingHandler;
		this.asyncReplies = delegatingHandler.isAsyncReplies();
	}

	/**
	 * Invoke the appropriate method for the payload.
	 * @param message the message.
	 * @param providedArgs additional arguments.
	 * @return the invocation result.
	 * @throws Exception if one occurs.
	 */
	public InvocationResult invoke(Message<?> message, @Nullable Object... providedArgs) throws Exception {
		InvocableHandlerMethod invokerHandlerMethodToUse = this.invokerHandlerMethod;
		if (invokerHandlerMethodToUse != null) {
			return new InvocationResult(invokerHandlerMethodToUse.invoke(message, providedArgs),
					null, invokerHandlerMethodToUse.getMethod().getGenericReturnType(),
					invokerHandlerMethodToUse.getBean(),
					invokerHandlerMethodToUse.getMethod());
		}
		Assert.notNull(this.delegatingHandler, "'delegatingHandler' or 'invokerHandlerMethod' is required");
		if (this.delegatingHandler.hasDefaultHandler()) {
			// Needed to avoid returning a raw Message which matches Object
			Object[] args = new Object[providedArgs.length + 1];
			args[0] = message.getPayload();
			System.arraycopy(providedArgs, 0, args, 1, providedArgs.length);
			return this.delegatingHandler.invoke(message, args);
		}
		else {
			return this.delegatingHandler.invoke(message, providedArgs);
		}
	}

	/**
	 * Get the method signature for the payload type via {@link Method#toGenericString()}.
	 * @param payload the payload.
	 * @return the method signature.
	 */
	public String getMethodAsString(Object payload) {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getMethod().toGenericString();
		}
		else {
			Assert.notNull(this.delegatingHandler, "'delegatingHandler' or 'invokerHandlerMethod' is required");
			return this.delegatingHandler.getMethodNameFor(payload);
		}
	}

	/**
	 * Get the method for the payload type.
	 * @param payload the payload.
	 * @return the method.
	 */
	public Method getMethodFor(Object payload) {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getMethod();
		}
		else {
			Assert.notNull(this.delegatingHandler, "'delegatingHandler' or 'invokerHandlerMethod' is required");
			return this.delegatingHandler.getMethodFor(payload);
		}
	}

	/**
	 * Return the return type for the method that will be chosen for this payload.
	 * @param payload the payload.
	 * @return the return type, or null if no handler found.
	 */
	public Type getReturnTypeFor(Object payload) {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getMethod().getReturnType();
		}
		else {
			Assert.notNull(this.delegatingHandler, "'delegatingHandler' or 'invokerHandlerMethod' is required");
			return this.delegatingHandler.getMethodFor(payload).getReturnType();
		}
	}

	/**
	 * Get the bean from the handler method.
	 * @return the bean.
	 */
	public Object getBean() {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getBean();
		}
		else {
			Assert.notNull(this.delegatingHandler, "'delegatingHandler' or 'invokerHandlerMethod' is required");
			return this.delegatingHandler.getBean();
		}
	}

	/**
	 * Get the method from the {@link InvocableHandlerMethod},
	 * otherwise {@code null}.
	 * @return the method, if any.
	 * @since 4.1
	 */
	public @Nullable Method getMethod() {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getMethod();
		}

		return null;
	}

	/**
	 * Return true if any handler method has an async reply type.
	 * @return the asyncReply.
	 */
	public boolean isAsyncReplies() {
		return this.asyncReplies;
	}

	/**
	 * Build an {@link InvocationResult} for the result and inbound payload.
	 * @param result the result.
	 * @param inboundPayload the payload.
	 * @return the invocation result.
	 */
	public @Nullable InvocationResult getInvocationResultFor(Object result, Object inboundPayload) {
		if (this.invokerHandlerMethod != null) {
			return new InvocationResult(result, null, this.invokerHandlerMethod.getMethod().getGenericReturnType(),
					this.invokerHandlerMethod.getBean(), this.invokerHandlerMethod.getMethod());
		}
		else {
			Assert.notNull(this.delegatingHandler, "'delegatingHandler' or 'invokerHandlerMethod' is required");
			return this.delegatingHandler.getInvocationResultFor(result, inboundPayload);
		}
	}

}
