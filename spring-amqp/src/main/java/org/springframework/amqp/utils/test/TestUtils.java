/*
 * Copyright 2013-present the original author or authors.
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

package org.springframework.amqp.utils.test;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.util.Assert;

/**
 * Testing utilities.
 *
 * @author Mark Fisher
 * @author Iwein Fuld
 * @author Oleg Zhurakousky
 * @author Gary Russell
 * @author Ngoc Nhan
 * @author Artem Bilan
 *
 * @since 1.2
 */
public final class TestUtils {

	private TestUtils() {
	}

	/**
	 * Uses nested {@link DirectFieldAccessor}s to obtain a property using dotted notation to traverse fields; e.g.
	 * "foo.bar.baz" will obtain a reference to the baz field of the bar field of foo. Adopted from Spring Integration.
	 * @param root The object.
	 * @param propertyPath The path.
	 * @return The field.
	 */
	public static @Nullable Object getPropertyValue(Object root, String propertyPath) {
		Object value = null;
		DirectFieldAccessor accessor = new DirectFieldAccessor(root);
		String[] tokens = propertyPath.split("\\.");
		for (int i = 0; i < tokens.length; i++) {
			value = accessor.getPropertyValue(tokens[i]);
			if (value != null) {
				if (i < tokens.length - 1) {
					accessor = new DirectFieldAccessor(value);
				}
				continue;
			}

			if (i == tokens.length - 1) {
				return null;
			}

			throw new IllegalArgumentException("intermediate property '" + tokens[i] + "' is null");
		}
		return value;
	}

	@Deprecated(since = "4.1", forRemoval = true)
	@SuppressWarnings("unchecked")
	public static <T> @Nullable T getPropertyValue(Object root, String propertyPath, Class<T> type) {
		Object value = propertyValue(root, propertyPath);
		if (value != null) {
			Assert.isAssignable(type, value.getClass());
		}
		return (T) value;
	}

	/**
	 * Uses nested {@link DirectFieldAccessor}s to get a property using dotted notation to traverse fields; e.g.
	 * {@code prop.subProp.subSubProp} will get a reference to the {@code subSubProp} field
	 * of the {@code subProp} field of {@code prop} prop from the {@code root}.
	 * @param root the object to get the property from.
	 * @param propertyPath the path to the property. Can be a dotted notation for a nested property.
	 * @param reified the argument which is used to extract a generic type for conversion.
	 *                Must not be set!
	 * @param <T> the expected type of the value.
	 * @return the property value.
	 * @since 4.1
	 */
	@SafeVarargs
	@SuppressWarnings({"varargs", "unchecked"})
	public static <T> @Nullable T propertyValue(Object root, String propertyPath, T... reified) {
		Assert.state(reified.length == 0,
				"No 'reified' parameter is allowed for 'propertyValue'. " +
						"The generic argument is enough for inferring the expected conversion type.");

		Object value = getPropertyValue(root, propertyPath);
		if (value != null) {
			Assert.isAssignable(getClassOf(reified), value.getClass());
		}
		return (T) value;
	}

	@SuppressWarnings("unchecked")
	private static <T> Class<T> getClassOf(T[] array) {
		return (Class<T>) array.getClass().getComponentType();
	}

}
