/*
 * Copyright 2014-present the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.core.NamedThreadLocal;
import org.springframework.util.Assert;

/**
 * Central helper that manages resources per thread to be used by resource management
 * code.
 * <p>
 * {@link #bind(Object, Object)} supports one resource per key without overwriting, that
 * is, a resource needs to be removed before a new one can be set for the same key. But
 * see {@link #push(Object, Object)} and {@link #pop(Object)}.
 * <p>
 * Resource management code should check for thread-bound resources via
 * {@link #has(Object)}.
 * <p>
 * This helper isn't designed for transaction synchronization cases. Use
 * {@code TransactionSynchronizationManager} and {@code ResourceHolder} instead.
 *
 * @author Artem Bilan
 * @author Gary Russell
 * @author Ngoc Nhan
 *
 * @since 1.3
 */
public final class SimpleResourceHolder {

	private static final String FOR_KEY = "] for key [";

	private static final String BOUND_TO_THREAD = "] bound to thread [";

	private static final Log LOGGER = LogFactory.getLog(SimpleResourceHolder.class);

	private static final ThreadLocal<@Nullable Map<Object, Object>> RESOURCES =
			new NamedThreadLocal<>("Simple resources");

	private static final ThreadLocal<@Nullable Map<Object, Deque<@Nullable Object>>> STACK =
			new NamedThreadLocal<>("Simple resources");

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
		Map<Object, Object> map = RESOURCES.get();
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
	public static @Nullable Object get(Object key) {
		Object value = doGet(key);
		if (value != null && LOGGER.isTraceEnabled()) {
			LOGGER.trace("Retrieved value [" + value + FOR_KEY + key + BOUND_TO_THREAD
					+ Thread.currentThread().getName() + "]");
		}
		return value;
	}

	/**
	 * Actually check the value of the resource that is bound for the given key.
	 * @param actualKey the key.
	 * @return the resource object.
	 */
	private static @Nullable Object doGet(Object actualKey) {
		Map<Object, Object> map = RESOURCES.get();
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
		Map<Object, Object> map = RESOURCES.get();
		// set ThreadLocal Map if none found
		if (map == null) {
			map = new HashMap<>();
			RESOURCES.set(map);
		}
		Object oldValue = map.put(key, value);
		Assert.isNull(oldValue, () -> "Already value [" + oldValue + FOR_KEY + key + BOUND_TO_THREAD
				+ Thread.currentThread().getName() + "]");

		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace(
					"Bound value [" + value + FOR_KEY + key + "] to thread [" + Thread.currentThread().getName() + "]");
		}
	}

	/**
	 * Set the value for this key and push any existing value onto a stack.
	 * @param key the key.
	 * @param value the value.
	 * @since 2.1.11
	 */
	public static void push(Object key, Object value) {
		Object currentValue = get(key);
		if (currentValue == null) {
			bind(key, value);
		}
		else {
			Map<Object, Deque<@Nullable Object>> stack = STACK.get();
			if (stack == null) {
				stack = new HashMap<>();
				STACK.set(stack);
			}
			stack.computeIfAbsent(key, k -> new LinkedList<>());
			stack.get(key).push(currentValue);
			unbind(key);
			bind(key, value);
		}
	}

	/**
	 * Unbind the current value and bind the head of the stack if present.
	 * @param key the key.
	 * @return the popped value.
	 * @since 2.1.11
	 */
	public static Object pop(Object key) {
		Object popped = unbind(key);
		Map<Object, Deque<@Nullable Object>> stack = STACK.get();
		if (stack != null) {
			Deque<@Nullable Object> deque = stack.get(key);
			if (deque != null && !deque.isEmpty()) {
				Object previousValue = deque.pop();
				if (previousValue != null) {
					bind(key, previousValue);
				}
				if (deque.isEmpty()) {
					STACK.remove();
				}
			}
		}
		return popped;
	}

	/**
	 * Unbind a resource for the given key from the current thread.
	 * @param key the key to unbind (usually the resource factory)
	 * @return the previously bound value (usually the active resource object)
	 * @throws IllegalStateException if there is no value bound to the thread
	 */
	public static Object unbind(Object key) throws IllegalStateException {
		Object value = unbindIfPossible(key);
		Assert.notNull(value,
				() -> "No value for key [" + key + BOUND_TO_THREAD + Thread.currentThread().getName() + "]");
		return value;
	}

	/**
	 * Unbind a resource for the given key from the current thread.
	 * @param key the key to unbind (usually the resource factory)
	 * @return the previously bound value, or <code>null</code> if none bound
	 */
	public static @Nullable Object unbindIfPossible(Object key) {
		Map<Object, Object> map = RESOURCES.get();
		if (map == null) {
			return null;
		}
		Object value = map.remove(key);
		// Remove entire ThreadLocal if empty...
		if (map.isEmpty()) {
			RESOURCES.remove();
		}

		if (value != null && LOGGER.isTraceEnabled()) {
			LOGGER.trace("Removed value [" + value + FOR_KEY + key + "] from thread ["
					+ Thread.currentThread().getName() + "]");
		}
		return value;
	}

	/**
	 * Clear resources for the current thread.
	 */
	public static void clear() {
		RESOURCES.remove();
		STACK.remove();
	}

	private SimpleResourceHolder() {
	}

}
