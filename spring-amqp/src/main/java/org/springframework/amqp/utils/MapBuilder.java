/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@code Builder} pattern implementation for a {@link Map}.
 * @param <B> the builder type.
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Artem Bilan
 * @author Gary Russell
 * @author Ngoc Nhan
 * @since 2.0
 */
public class MapBuilder<B extends MapBuilder<B, K, V>, K, V> {

	private final Map<K, V> map = new HashMap<>();

	public B put(K key, V value) {
		this.map.put(key, value);
		return _this();
	}

	public Map<K, V> get() {
		return this.map;
	}

	@SuppressWarnings("unchecked")
	protected final B _this() { // NOSONAR underscore
		return (B) this;
	}

}
