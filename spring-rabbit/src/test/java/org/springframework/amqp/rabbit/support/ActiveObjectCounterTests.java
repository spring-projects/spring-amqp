/*
 * Copyright 2002-present the original author or authors.
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
