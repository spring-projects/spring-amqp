/*
 * Copyright 2016-2025 the original author or authors.
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

package org.springframework.amqp.rabbit.test.mockito;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;
import org.mockito.internal.stubbing.defaultanswers.ForwardsInvocations;
import org.mockito.invocation.InvocationOnMock;

/**
 * An {@link org.mockito.stubbing.Answer} to optionally call the real method and allow
 * returning a custom result. Captures any exceptions thrown.
 *
 * @param <T> the return type.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.6
 *
 */
@SuppressWarnings("serial")
public class LambdaAnswer<T> extends ForwardsInvocations {

	private final boolean callRealMethod;

	private final ValueToReturn<T> callback;

	private final Set<Exception> exceptions = ConcurrentHashMap.newKeySet();

	private final boolean hasDelegate;

	/**
	 * Construct an instance with the provided properties. Use the test harness to get an
	 * instance with the proper delegate.
	 * @param callRealMethod true to call the real method.
	 * @param callback the call back to receive the result.
	 * @param delegate the delegate.
	 */
	public LambdaAnswer(boolean callRealMethod, ValueToReturn<T> callback, @Nullable Object delegate) {
		super(delegate);
		this.callRealMethod = callRealMethod;
		this.callback = callback;
		this.hasDelegate = delegate != null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T answer(InvocationOnMock invocation) throws Throwable {
		T result = null;
		try {
			if (this.callRealMethod) {
				if (this.hasDelegate) {
					result = (T) super.answer(invocation);
				}
				else {
					result = (T) invocation.callRealMethod();
				}
			}
			return this.callback.apply(invocation, result);
		}
		catch (Exception e) {
			this.exceptions.add(e);
			throw e;
		}
	}

	/**
	 * Return the exceptions thrown, if any.
	 * @return the exceptions.
	 * @since 2.2.3
	 */
	public Collection<Exception> getExceptions() {
		return new LinkedHashSet<>(this.exceptions);
	}

	@FunctionalInterface
	public interface ValueToReturn<T> {

		T apply(InvocationOnMock invocation, @Nullable T result);

	}

}
