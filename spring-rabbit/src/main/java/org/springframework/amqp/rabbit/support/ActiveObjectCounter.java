/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.support;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A mechanism to keep track of active objects.
 * @param <T> the object type.
 *
 * @author Dave Syer
 * @author Artem Bilan
 * @author Ngoc Nhan
 *
 */
public class ActiveObjectCounter<T> {

	private final ConcurrentMap<T, CountDownLatch> locks = new ConcurrentHashMap<>();

	private volatile boolean active = true;

	public void add(T object) {
		CountDownLatch lock = new CountDownLatch(1);
		this.locks.putIfAbsent(object, lock);
	}

	public void release(T object) {
		CountDownLatch remove = this.locks.remove(object);
		if (remove != null) {
			remove.countDown();
		}
	}

	public boolean await(long timeout, TimeUnit timeUnit) throws InterruptedException {
		long t0 = System.currentTimeMillis();
		long t1 = t0 + TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
		while (System.currentTimeMillis() <= t1) {
			if (this.locks.isEmpty()) {
				return true;
			}
			Collection<T> objects = new HashSet<>(this.locks.keySet());
			for (T object : objects) {
				CountDownLatch lock = this.locks.get(object);
				if (lock == null) {
					continue;
				}
				t0 = System.currentTimeMillis();
				if (lock.await(t1 - t0, TimeUnit.MILLISECONDS)) {
					this.locks.remove(object);
				}
			}
		}
		return false;
	}

	public int getCount() {
		return this.locks.size();
	}

	public void reset() {
		this.locks.clear();
		this.active = true;
	}

	public void deactivate() {
		this.active = false;
	}

	public boolean isActive() {
		return this.active;
	}

}
