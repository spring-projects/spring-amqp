/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.listener.adapter;

import java.lang.reflect.Method;

import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

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

	public HandlerAdapter(InvocableHandlerMethod invokerHandlerMethod) {
		this.invokerHandlerMethod = invokerHandlerMethod;
		this.delegatingHandler = null;
	}

	public HandlerAdapter(DelegatingInvocableHandler delegatingHandler) {
		this.invokerHandlerMethod = null;
		this.delegatingHandler = delegatingHandler;
	}

	public Object invoke(Message<?> message, Object... providedArgs) throws Exception {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.invoke(message, providedArgs);
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

	public String getMethodAsString(Object payload) {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getMethod().toGenericString();
		}
		else {
			return this.delegatingHandler.getMethodNameFor(payload);
		}
	}

	/**
	 * Return the return type for the method that will be chosen for this payload.
	 * @param payload the payload.
	 * @return the return type, or null if no handler found.
	 * @since 2.0
	 */
	public Object getReturnType(Object payload) {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getMethod().getReturnType();
		}
		else {
			Method method = this.delegatingHandler.getMethodFor(payload);
			return method == null ? null : method.getReturnType();
		}
	}

	public Object getBean() {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getBean();
		}
		else {
			return this.delegatingHandler.getBean();
		}
	}


}
