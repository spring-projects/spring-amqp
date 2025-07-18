/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.amqp.utils;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;

import org.springframework.util.StringUtils;

/**
 * Chained utility methods to simplify some Java repetitive code. Obtain a reference to
 * the singleton {@link #INSTANCE} and then chain calls to the utility methods.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.1.4
 *
 */
public final class JavaUtils {

	/**
	 * The singleton instance of this utility class.
	 */
	public static final JavaUtils INSTANCE = new JavaUtils();

	private JavaUtils() {
	}

	/**
	 * Invoke {@link Consumer#accept(Object)} with the value if it is not null and the condition is true.
	 * @param condition the condition.
	 * @param value the value. Skipped if null.
	 * @param consumer the consumer.
	 * @param <T> the value type.
	 * @return this.
	 */
	public <T> JavaUtils acceptIfCondition(boolean condition, @Nullable T value, Consumer<T> consumer) {
		if (condition && value != null) {
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
	public <T> JavaUtils acceptIfNotNull(@Nullable T value, Consumer<T> consumer) {
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
	public JavaUtils acceptIfHasText(@Nullable String value, Consumer<String> consumer) {
		if (StringUtils.hasText(value)) {
			consumer.accept(value);
		}
		return this;
	}

	/**
	 * Invoke {@link BiConsumer#accept(Object, Object)} with the arguments if the
	 * condition is true.
	 * @param condition the condition.
	 * @param t1 the first consumer argument
	 * @param t2 the second consumer argument
	 * @param consumer the consumer.
	 * @param <T1> the first argument type.
	 * @param <T2> the second argument type.
	 * @return this.
	 */
	public <T1, T2> JavaUtils acceptIfCondition(boolean condition, T1 t1, T2 t2, BiConsumer<T1, T2> consumer) {
		if (condition) {
			consumer.accept(t1, t2);
		}
		return this;
	}

	/**
	 * Invoke {@link BiConsumer#accept(Object, Object)} with the arguments if the t2
	 * argument is not null.
	 * @param t1 the first argument
	 * @param t2 the second consumer argument
	 * @param consumer the consumer.
	 * @param <T1> the first argument type.
	 * @param <T2> the second argument type.
	 * @return this.
	 */
	public <T1, T2> JavaUtils acceptIfNotNull(T1 t1, @Nullable T2 t2, BiConsumer<T1, T2> consumer) {
		if (t2 != null) {
			consumer.accept(t1, t2);
		}
		return this;
	}

	/**
	 * Invoke {@link BiConsumer#accept(Object, Object)} with the arguments if the value
	 * argument is not null or empty.
	 * @param t1 the first consumer argument.
	 * @param value the second consumer argument
	 * @param <T> the first argument type.
	 * @param consumer the consumer.
	 * @return this.
	 */
	public <T> JavaUtils acceptIfHasText(T t1, @Nullable String value, BiConsumer<T, String> consumer) {
		if (StringUtils.hasText(value)) {
			consumer.accept(t1, value);
		}
		return this;
	}


	/**
	 * Invoke {@link Consumer#accept(Object)} with the value or alternative if one of them is not null.
	 * @param value the value.
	 * @param alternative the other value if the {@code value} argument is null.
	 * @param consumer the consumer.
	 * @param <T> the value type.
	 * @return this.
	 * @since 4.0
	 */
	public <T> JavaUtils acceptOrElseIfNotNull(@Nullable T value, @Nullable T alternative, Consumer<T> consumer) {
		if (value != null) {
			consumer.accept(value);
		}
		else if (alternative != null) {
			consumer.accept(alternative);
		}
		return this;
	}

}
