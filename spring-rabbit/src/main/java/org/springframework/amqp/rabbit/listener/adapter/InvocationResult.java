/*
 * Copyright 2018-2019 the original author or authors.
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

import org.springframework.expression.Expression;
import org.springframework.lang.Nullable;

/**
 * The result of a listener method invocation.
 *
 * @author Gary Russell
 *
 * @since 2.1
 */
public final class InvocationResult {

	private final Object returnValue;

	private final Expression sendTo;

	@Nullable
	private final Type returnType;

	@Nullable
	private final Object bean;

	@Nullable
	private final Method method;

	/**
	 * @deprecated in favor of {@link #InvocationResult(Object, Expression, Type, Object, Method)}.
	 * @param result the result.
	 * @param sendTo the sendTo expression.
	 * @param returnType the return type.
	 */
	@Deprecated
	public InvocationResult(Object result, @Nullable Expression sendTo, @Nullable Type returnType) {
		this(result, sendTo, returnType, null, null);
	}

	public InvocationResult(Object result, @Nullable Expression sendTo, @Nullable Type returnType,
			@Nullable Object bean, @Nullable Method method) {

		this.returnValue = result;
		this.sendTo = sendTo;
		this.returnType = returnType;
		this.bean = bean;
		this.method = method;
	}

	public Object getReturnValue() {
		return this.returnValue;
	}

	public Expression getSendTo() {
		return this.sendTo;
	}

	@Nullable
	public Type getReturnType() {
		return this.returnType;
	}

	@Nullable
	public Object getBean() {
		return this.bean;
	}

	@Nullable
	public Method getMethod() {
		return this.method;
	}

	@Override
	public String toString() {
		return "InvocationResult [returnValue=" + this.returnValue
				+ (this.sendTo != null ? ", sendTo=" + this.sendTo : "")
				+ ", returnType=" + this.returnType
				+ ", bean=" + this.bean
				+ ", method=" + this.method
				+ "]";
	}

}
