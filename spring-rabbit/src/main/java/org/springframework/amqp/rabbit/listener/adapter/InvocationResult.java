/*
 * Copyright 2018 the original author or authors.
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

import java.lang.reflect.Type;

import org.springframework.expression.Expression;

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

	private final Type returnType;

	public InvocationResult(Object result, Expression sendTo, Type returnType) {
		this.returnValue = result;
		this.sendTo = sendTo;
		this.returnType = returnType;
	}

	public Object getReturnValue() {
		return this.returnValue;
	}

	public Expression getSendTo() {
		return this.sendTo;
	}


	public Type getReturnType() {
		return this.returnType;
	}

	@Override
	public String toString() {
		return "InvocationResult [returnValue=" + this.returnValue
				+ (this.sendTo != null ? ", sendTo=" + this.sendTo : "")
				+ ", returnType=" + this.returnType + "]";
	}

}
