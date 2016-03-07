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

import java.util.concurrent.CountDownLatch;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * An Answer for void returning methods that calls the real method and
 * counts down a latch.
 *
 * @author Gary Russell
 * @since 1.6
 *
 */
public class LatchCountDownAndCallRealMethodAnswer implements Answer<Void> {

	private final CountDownLatch latch;

	/**
	 * @param count to set in a {@link CountDownLatch}.
	 */
	public LatchCountDownAndCallRealMethodAnswer(int count) {
		this.latch = new CountDownLatch(count);
	}

	@Override
	public Void answer(InvocationOnMock invocation) throws Throwable {
		invocation.callRealMethod();
		this.latch.countDown();
		return null;
	}


	public CountDownLatch getLatch() {
		return latch;
	}

}
