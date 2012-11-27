/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.springframework.amqp.rabbit.listener;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Dave Syer
 *
 */
public class ActiveObjectCounter<T> {

	private final ConcurrentMap<T, CountDownLatch> locks = new ConcurrentHashMap<T, CountDownLatch>();

	public void add(T object) {
		CountDownLatch lock = new CountDownLatch(1);
		locks.putIfAbsent(object, lock);
	}

	public void release(T object) {
		CountDownLatch remove = locks.remove(object);
		if (remove != null) {
			remove.countDown();
		}
	}

	public boolean await(Long timeout, TimeUnit timeUnit) throws InterruptedException {
		long t0 = System.currentTimeMillis();
		long t1 = t0 + TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
		while (System.currentTimeMillis() <= t1) {
			if (locks.isEmpty()) {
				return true;
			}
			Collection<T> objects = new HashSet<T>(locks.keySet());
			for (T object : objects) {
				CountDownLatch lock = locks.get(object);
				if (lock==null) {
					continue;
				}
				t0 = System.currentTimeMillis();
				if (lock.await(t1 - t0, TimeUnit.MILLISECONDS)) {
					locks.remove(object);
				}
			}
		}
		return false;
	}

	public int getCount() {
		return locks.size();
	}

	public void reset() {
		locks.clear();
	}

}
