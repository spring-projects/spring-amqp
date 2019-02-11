/*
 * Copyright 2019 the original author or authors.
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

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.springframework.util.StringUtils;

/**
 * Chained utility methods to simplify some Java repetitive code. Obtain a reference to
 * the singleton {@link #INSTANCE} and then chain calls to the utility methods.
 *
 * @author Gary Russell
 * @since 2.1.4
 *
 */
public final class JavaUtils {

	/**
	 * The singleton instance of this utility class.
	 */
	public static final JavaUtils INSTANCE = new JavaUtils();

	private JavaUtils() {
		super();
	}

	/**
	 * Invoke {@link Consumer#accept(Object)} with the value if the condition is true.
	 * @param condition the condition.
	 * @param value the value.
	 * @param consumer the consumer.
	 * @param <T> the value type.
	 * @return this.
	 */
	public <T> JavaUtils acceptIfCondition(boolean condition, T value, Consumer<T> consumer) {
		if (condition) {
			consumer.accept(value);
		}
		return this;
	}

	/**
	 * Invoke {@link Consumer#accept(Object)} with the value if it is not null.
	 * @param value the value.
	 * @param consumer the consumer.
	 * @param <T> the value type.
	 * @return this.
	 */
	public <T> JavaUtils acceptIfNotNull(T value, Consumer<T> consumer) {
		if (value != null) {
			consumer.accept(value);
		}
		return this;
	}

	/**
	 * Invoke {@link Consumer#accept(Object)} with the value if it is not null or empty.
	 * @param value the value.
	 * @param consumer the consumer.
	 * @return this.
	 */
	public JavaUtils acceptIfHasText(String value, Consumer<String> consumer) {
		if (StringUtils.hasText(value)) {
			consumer.accept(value);
		}
		return this;
	}

	/**
	 * Invoke {@link BiConsumer#accept(Object, Object)} with the arguments if the
	 * condition is true.
	 * @param condition the condition.
	 * @param value the second consumer argument
	 * @param t the first consumer argument
	 * @param consumer the consumer.
	 * @param <T> the first argument type.
	 * @param <U> the second argument type.
	 * @return this.
	 */
	public <T, U> JavaUtils acceptIfCondition(boolean condition, U value, T t, BiConsumer<T, U> consumer) {
		if (condition) {
			consumer.accept(t, value);
		}
		return this;
	}

	/**
	 * Invoke {@link BiConsumer#accept(Object, Object)} with the arguments if the value
	 * argument is not null.
	 * @param value the second consumer argument
	 * @param t the first argument
	 * @param consumer the consumer.
	 * @param <T> the first argument type.
	 * @param <U> the second argument type.
	 * @return this.
	 */
	public <T, U> JavaUtils acceptIfNotNull(U value, T t, BiConsumer<T, U> consumer) {
		if (value != null) {
			consumer.accept(t, value);
		}
		return this;
	}

	/**
	 * Invoke {@link BiConsumer#accept(Object, Object)} with the value if it is not null
	 * or empty.
	 * @param value the second consumer argument
	 * @param t the first consumer argument.
	 * @param <T> the first argument type.
	 * @param consumer the consumer.
	 * @return this.
	 */
	public <T> JavaUtils acceptIfHasText(String value, T t, BiConsumer<T, String> consumer) {
		if (StringUtils.hasText(value)) {
			consumer.accept(t, value);
		}
		return this;
	}

}
