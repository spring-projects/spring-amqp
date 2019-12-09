/*
 * Copyright 2016-2019 the original author or authors.
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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.springframework.lang.Nullable;

/**
 * An {@link Answer} for void returning methods that calls the real method and counts down
 * a latch. Captures any exceptions thrown.
 *
 * @author Gary Russell
 * @since 1.6
 *
 */
public class LatchCountDownAndCallRealMethodAnswer implements Answer<Void> {

	private final CountDownLatch latch;

	private final Set<Exception> exceptions = Collections.synchronizedSet(new LinkedHashSet<>());

	/**
	 * @param count to set in a {@link CountDownLatch}.
	 */
	public LatchCountDownAndCallRealMethodAnswer(int count) {
		this.latch = new CountDownLatch(count);
	}

	@Override
	public Void answer(InvocationOnMock invocation) throws Throwable {
		try {
			invocation.callRealMethod();
		}
		catch (Exception e) {
			this.exceptions.add(e);
			throw e;
		}
		finally {
			this.latch.countDown();
		}
		return null;
	}


	public CountDownLatch getLatch() {
		return latch;
	}

	/**
	 * Return the exceptions thrown.
	 * @return the exceptions.
	 * @since 2.2.3
	 */
	@Nullable
	public Collection<Exception> getExceptions() {
		return Collections.unmodifiableCollection(this.exceptions);
	}

}
