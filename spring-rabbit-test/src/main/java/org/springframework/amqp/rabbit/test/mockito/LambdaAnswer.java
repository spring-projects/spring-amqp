/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.amqp.rabbit.test.mockito;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * An Answer to optionally call the real method and allow returning a
 * custom result.
 *
 * @author Gary Russell
 * @since 1.6
 *
 */
public class LambdaAnswer<T> implements Answer<T> {

	private final boolean callRealMethod;

	private final ValueToReturn<T> callback;

	public LambdaAnswer(boolean callRealMethod, ValueToReturn<T> callback) {
		this.callRealMethod = callRealMethod;
		this.callback = callback;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T answer(InvocationOnMock invocation) throws Throwable {
		T result = null;
		if (this.callRealMethod) {
			result = (T) invocation.callRealMethod();
		}
		return this.callback.apply(invocation, result);
	}

	public interface ValueToReturn<T> {

		T apply(InvocationOnMock invocation, T result);

	}

}
