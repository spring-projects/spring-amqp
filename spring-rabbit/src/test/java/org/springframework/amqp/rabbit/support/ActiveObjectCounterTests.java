/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.rabbit.support;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Dave Syer
 * @author Gary Russell
 *
 */
public class ActiveObjectCounterTests {

	private final ActiveObjectCounter<Object> counter = new ActiveObjectCounter<Object>();

	@Test
	public void testActiveCount() {
		final Object object1 = new Object();
		final Object object2 = new Object();
		counter.add(object1);
		counter.add(object2);
		assertThat(counter.getCount()).isEqualTo(2);
		counter.release(object2);
		assertThat(counter.getCount()).isEqualTo(1);
		counter.release(object1);
		counter.release(object1);
		assertThat(counter.getCount()).isEqualTo(0);
	}

	@Test
	public void testWaitForLocks() throws Exception {
		final Object object1 = new Object();
		final Object object2 = new Object();
		counter.add(object1);
		counter.add(object2);
		Future<Boolean> future = Executors.newSingleThreadExecutor().submit(() -> {
			counter.release(object1);
			counter.release(object2);
			counter.release(object2);
			return true;
		});
		assertThat(counter.await(1000L, TimeUnit.MILLISECONDS)).isEqualTo(true);
		assertThat(future.get()).isEqualTo(true);
	}

	@Test
	public void testTimeoutWaitForLocks() throws Exception {
		final Object object1 = new Object();
		counter.add(object1);
		assertThat(counter.await(200L, TimeUnit.MILLISECONDS)).isEqualTo(false);
	}

}
