/*
 * Copyright 2016-2017 the original author or authors.
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
 * @since 2.0
 */
public class MapBuilder<B extends MapBuilder<B, K, V>, K, V> {

	private final Map<K, V> map = new HashMap<K, V>();

	public B put(K key, V value) {
		this.map.put(key, value);
		return _this();
	}

	public Map<K, V> get() {
		return this.map;
	}

	@SuppressWarnings("unchecked")
	protected final B _this() {
		return (B) this;
	}

}
