/*
 * Copyright 2016-2023 the original author or authors.
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.mockito.internal.stubbing.defaultanswers.ForwardsInvocations;
import org.mockito.invocation.InvocationOnMock;

import org.springframework.lang.Nullable;

/**
 * An {@link org.mockito.stubbing.Answer} for void returning methods that calls the real
 * method and counts down a latch. Captures any exceptions thrown.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.6
 *
 */
public class LatchCountDownAndCallRealMethodAnswer extends ForwardsInvocations {

	private static final long serialVersionUID = 1L;

	private final transient CountDownLatch latch;

	private final Set<Exception> exceptions = ConcurrentHashMap.newKeySet();

	private final boolean hasDelegate;

	/**
	 * Get an instance with the provided properties. Use the test harness to get an
	 * instance with the proper delegate.
	 * @param count the count.
	 * @param delegate the delegate.
	 * @since 2.1.16
	 */
	public LatchCountDownAndCallRealMethodAnswer(int count, @Nullable Object delegate) {
		super(delegate);
		this.latch = new CountDownLatch(count);
		this.hasDelegate = delegate != null;
	}

	@Override
	public Object answer(InvocationOnMock invocation) throws Throwable {
		try {
			if (this.hasDelegate) {
				return super.answer(invocation);
			}
			else {
				invocation.callRealMethod();
			}
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

	/**
	 * Wait for the latch to count down.
	 * @param timeout the timeout in seconds.
	 * @return the result of awaiting on the latch; true if counted down.
	 * @throws InterruptedException if the thread is interrupted.
	 * @since 2.1.16
	 */
	public boolean await(int timeout) throws InterruptedException {
		return this.latch.await(timeout, TimeUnit.SECONDS);
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
		return new LinkedHashSet<>(this.exceptions);
	}

}
