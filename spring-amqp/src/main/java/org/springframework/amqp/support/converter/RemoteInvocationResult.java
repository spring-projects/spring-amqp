/*
 * Copyright 2021 the original author or authors.
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

package org.springframework.amqp.support.converter;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

import org.springframework.lang.Nullable;

/**
 * Encapsulates a remote invocation result, holding a result value or an exception.
 *
 * @author Juergen Hoeller
 * @author Gary Russell
 * @since 3.0
 */
public class RemoteInvocationResult implements Serializable {

	/** Use serialVersionUID from Spring 1.1 for interoperability. */
	private static final long serialVersionUID = 2138555143707773549L;


	@Nullable
	private Object value;

	@Nullable
	private Throwable exception;


	/**
	 * Create a new RemoteInvocationResult for the given result value.
	 * @param value the result value returned by a successful invocation
	 * of the target method
	 */
	public RemoteInvocationResult(@Nullable Object value) {
		this.value = value;
	}

	/**
	 * Create a new RemoteInvocationResult for the given exception.
	 * @param exception the exception thrown by an unsuccessful invocation
	 * of the target method
	 */
	public RemoteInvocationResult(@Nullable Throwable exception) {
		this.exception = exception;
	}

	/**
	 * Create a new RemoteInvocationResult for JavaBean-style deserialization
	 * (e.g. with Jackson).
	 * @see #setValue
	 * @see #setException
	 */
	public RemoteInvocationResult() {
	}


	/**
	 * Set the result value returned by a successful invocation of the
	 * target method, if any.
	 * <p>This setter is intended for JavaBean-style deserialization.
	 * Use {@link #RemoteInvocationResult(Object)} otherwise.
	 * @see #RemoteInvocationResult()
	 */
	public void setValue(@Nullable Object value) {
		this.value = value;
	}

	/**
	 * Return the result value returned by a successful invocation
	 * of the target method, if any.
	 * @see #hasException
	 */
	@Nullable
	public Object getValue() {
		return this.value;
	}

	/**
	 * Set the exception thrown by an unsuccessful invocation of the
	 * target method, if any.
	 * <p>This setter is intended for JavaBean-style deserialization.
	 * Use {@link #RemoteInvocationResult(Throwable)} otherwise.
	 * @see #RemoteInvocationResult()
	 */
	public void setException(@Nullable Throwable exception) {
		this.exception = exception;
	}

	/**
	 * Return the exception thrown by an unsuccessful invocation
	 * of the target method, if any.
	 * @see #hasException
	 */
	@Nullable
	public Throwable getException() {
		return this.exception;
	}

	/**
	 * Return whether this invocation result holds an exception.
	 * If this returns {@code false}, the result value applies
	 * (even if it is {@code null}).
	 * @see #getValue
	 * @see #getException
	 */
	public boolean hasException() {
		return (this.exception != null);
	}

	/**
	 * Return whether this invocation result holds an InvocationTargetException,
	 * thrown by an invocation of the target method itself.
	 * @see #hasException()
	 */
	public boolean hasInvocationTargetException() {
		return (this.exception instanceof InvocationTargetException);
	}


	/**
	 * Recreate the invocation result, either returning the result value
	 * in case of a successful invocation of the target method, or
	 * rethrowing the exception thrown by the target method.
	 * @return the result value, if any
	 * @throws Throwable the exception, if any
	 */
	@Nullable
	public Object recreate() throws Throwable {
		if (this.exception != null) {
			Throwable exToThrow = this.exception;
			if (this.exception instanceof InvocationTargetException) {
				exToThrow = ((InvocationTargetException) this.exception).getTargetException();
			}
			RemoteInvocationUtils.fillInClientStackTraceIfPossible(exToThrow);
			throw exToThrow;
		}
		else {
			return this.value;
		}
	}

}
