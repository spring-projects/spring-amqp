/*
 * Copyright 2002-2017 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

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
 *
 */
public class ActiveObjectCounter<T> {

	private final ConcurrentMap<T, CountDownLatch> locks = new ConcurrentHashMap<T, CountDownLatch>();

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
			Collection<T> objects = new HashSet<T>(this.locks.keySet());
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
