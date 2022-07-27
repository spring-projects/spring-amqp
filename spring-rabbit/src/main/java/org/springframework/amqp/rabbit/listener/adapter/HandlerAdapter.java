/*
 * Copyright 2015-2022 the original author or authors.
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

package org.springframework.amqp.rabbit.listener.adapter;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.concurrent.CompletableFuture;

import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * A wrapper for either an {@link InvocableHandlerMethod} or
 * {@link DelegatingInvocableHandler}. All methods delegate to the
 * underlying handler.
 *
 * @author Gary Russell
 * @since 1.5
 *
 */
public class HandlerAdapter {

	private final InvocableHandlerMethod invokerHandlerMethod;

	private final DelegatingInvocableHandler delegatingHandler;

	private final boolean asyncReplies;

	/**
	 * Construct an instance with the provided method.
	 * @param invokerHandlerMethod the method.
	 */
	public HandlerAdapter(InvocableHandlerMethod invokerHandlerMethod) {
		this.invokerHandlerMethod = invokerHandlerMethod;
		this.delegatingHandler = null;
		this.asyncReplies = (AbstractAdaptableMessageListener.monoPresent
				&& MonoHandler.isMono(invokerHandlerMethod.getMethod().getReturnType()))
			|| ListenableFuture.class.isAssignableFrom(invokerHandlerMethod.getMethod().getReturnType())
			|| CompletableFuture.class.isAssignableFrom(invokerHandlerMethod.getMethod().getReturnType());
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
	public InvocationResult invoke(@Nullable Message<?> message, Object... providedArgs) throws Exception { // NOSONAR
		if (this.invokerHandlerMethod != null) { // NOSONAR (nullable message)
			return new InvocationResult(this.invokerHandlerMethod.invoke(message, providedArgs),
					null, this.invokerHandlerMethod.getMethod().getGenericReturnType(),
					this.invokerHandlerMethod.getBean(),
					this.invokerHandlerMethod.getMethod());
		}
		else if (this.delegatingHandler.hasDefaultHandler()) {
			// Needed to avoid returning raw Message which matches Object
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
			return this.delegatingHandler.getMethodNameFor(payload);
		}
	}

	/**
	 * Get the method for the payload type.
	 * @param payload the payload.
	 * @return the method.
	 * @since 2.2.3
	 */
	public Method getMethodFor(Object payload) {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getMethod();
		}
		else {
			return this.delegatingHandler.getMethodFor(payload);
		}
	}

	/**
	 * Return the return type for the method that will be chosen for this payload.
	 * @param payload the payload.
	 * @return the return type, or null if no handler found.
	 * @since 2.2.3
	 */
	public Type getReturnTypeFor(Object payload) {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getMethod().getReturnType();
		}
		else {
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
			return this.delegatingHandler.getBean();
		}
	}

	/**
	 * Return true if any handler method has an async reply type.
	 * @return the asyncReply.
	 * @since 2.2.21
	 */
	public boolean isAsyncReplies() {
		return this.asyncReplies;
	}

	/**
	 * Build an {@link InvocationResult} for the result and inbound payload.
	 * @param result the result.
	 * @param inboundPayload the payload.
	 * @return the invocation result.
	 * @since 2.1.7
	 */
	@Nullable
	public InvocationResult getInvocationResultFor(Object result, Object inboundPayload) {
		if (this.invokerHandlerMethod != null) {
			return new InvocationResult(result, null, this.invokerHandlerMethod.getMethod().getGenericReturnType(),
					this.invokerHandlerMethod.getBean(), this.invokerHandlerMethod.getMethod());
		}
		else {
			return this.delegatingHandler.getInvocationResultFor(result, inboundPayload);
		}
	}

}
