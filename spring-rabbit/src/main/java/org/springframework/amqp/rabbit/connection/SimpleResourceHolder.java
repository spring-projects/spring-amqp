/*
 * Copyright 2014-2017 the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.core.NamedThreadLocal;
import org.springframework.util.Assert;

/**
 * Central helper that manages resources per thread to be used by resource management code.
 *
 * <p>Supports one resource per key without overwriting, that is, a resource needs
 * to be removed before a new one can be set for the same key.
 *
 * <p>Resource management code should check for thread-bound resources via {@link #has(Object)}.
 *
 * <p>This helper isn't designed for transaction synchronization cases.
 * Use {@code TransactionSynchronizationManager} and {@code ResourceHolder} instead.
 *
 * @author Artem Bilan
 * @since 1.3
 */
public final class SimpleResourceHolder {

	private static final Log logger = LogFactory.getLog(SimpleResourceHolder.class);

	private static final ThreadLocal<Map<Object, Object>> resources = new NamedThreadLocal<Map<Object, Object>>("Simple resources");


	/**
	 * Return all resources that are bound to the current thread.
	 * <p>Mainly for debugging purposes. Resource managers should always invoke
	 * <code>hasResource</code> for a specific resource key that they are interested in.
	 * @return a Map with resource keys (usually the resource factory) and resource
	 * values (usually the active resource object), or an empty Map if there are
	 * currently no resources bound
	 * @see #has
	 */
	public static Map<Object, Object> getResources() {
		Map<Object, Object> map = resources.get();
		return (map != null ? Collections.unmodifiableMap(map) : Collections.emptyMap());
	}

	/**
	 * Check if there is a resource for the given key bound to the current thread.
	 * @param key the key to check (usually the resource factory)
	 * @return if there is a value bound to the current thread
	 */
	public static boolean has(Object key) {
		Object value = doGet(key);
		return (value != null);
	}

	/**
	 * Retrieve a resource for the given key that is bound to the current thread.
	 * @param key the key to check (usually the resource factory)
	 * @return a value bound to the current thread (usually the active
	 * resource object), or <code>null</code> if none
	 */
	public static Object get(Object key) {
		Object value = doGet(key);
		if (value != null && logger.isTraceEnabled()) {
			logger.trace("Retrieved value [" + value + "] for key [" + key + "] bound to thread [" + Thread.currentThread().getName() + "]");
		}
		return value;
	}

	/**
	 * Actually check the value of the resource that is bound for the given key.
	 * @param actualKey the key.
	 * @return the resource object.
	 */
	private static Object doGet(Object actualKey) {
		Map<Object, Object> map = resources.get();
		if (map == null) {
			return null;
		}
		return map.get(actualKey);
	}

	/**
	 * Bind the given resource for the given key to the current thread.
	 * @param key the key to bind the value to (usually the resource factory)
	 * @param value the value to bind (usually the active resource object)
	 * @throws IllegalStateException if there is already a value bound to the thread
	 */
	public static void bind(Object key, Object value) {
		Assert.notNull(value, "Value must not be null");
		Map<Object, Object> map = resources.get();
		// set ThreadLocal Map if none found
		if (map == null) {
			map = new HashMap<Object, Object>();
			resources.set(map);
		}
		Object oldValue = map.put(key, value);
		Assert.isNull(oldValue, () -> "Already value [" + oldValue + "] for key [" + key + "] bound to thread [" + Thread.currentThread().getName() + "]");

		if (logger.isTraceEnabled()) {
			logger.trace("Bound value [" + value + "] for key [" + key + "] to thread [" + Thread.currentThread().getName() + "]");
		}
	}

	/**
	 * Unbind a resource for the given key from the current thread.
	 * @param key the key to unbind (usually the resource factory)
	 * @return the previously bound value (usually the active resource object)
	 * @throws IllegalStateException if there is no value bound to the thread
	 */
	public static Object unbind(Object key) throws IllegalStateException {
		Object value = unbindIfPossible(key);
		Assert.notNull(value, () -> "No value for key [" + key + "] bound to thread [" + Thread.currentThread().getName() + "]");
		return value;
	}

	/**
	 * Unbind a resource for the given key from the current thread.
	 * @param key the key to unbind (usually the resource factory)
	 * @return the previously bound value, or <code>null</code> if none bound
	 */
	public static Object unbindIfPossible(Object key) {
		Map<Object, Object> map = resources.get();
		if (map == null) {
			return null;
		}
		Object value = map.remove(key);
		// Remove entire ThreadLocal if empty...
		if (map.isEmpty()) {
			resources.remove();
		}

		if (value != null && logger.isTraceEnabled()) {
			logger.trace("Removed value [" + value + "] for key [" + key + "] from thread [" + Thread.currentThread().getName() + "]");
		}
		return value;
	}

	/**
	 * Clear resources for the current thread.
	 */
	public static void clear() {
		resources.remove();
	}

	private SimpleResourceHolder() {
	}

}
