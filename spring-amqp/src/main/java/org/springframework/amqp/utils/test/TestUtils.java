/*
 * Copyright 2013-present the original author or authors.
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
	 * @param <T> the expected type of the value.
	 * @return The field.
	 */
	@SuppressWarnings("unchecked")
	public static <T> @Nullable T getPropertyValue(Object root, String propertyPath) {
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
		return (T) value;
	}

	/**
	 * Uses nested {@link DirectFieldAccessor}s to get a property using dotted notation to traverse fields; e.g.
	 * {@code prop.subProp.subSubProp} will get a reference to the {@code subSubProp} field
	 * of the {@code subProp} field of {@code prop} prop from the {@code root}.
	 * @param root the object to get the property from.
	 * @param propertyPath the path to the property. Can be a dotted notation for a nested property.
	 * @param type the value expected type.
	 * @param <T> the expected type of the value.
	 * @return the property value.
	 * @deprecated since 4.1, use {@link #getPropertyValue(Object, String)} instead:
	 * there is no need in extra type check in tests.
	 */
	@Deprecated(since = "4.1", forRemoval = true)
	@SuppressWarnings("unchecked")
	public static <T> @Nullable T getPropertyValue(Object root, String propertyPath, Class<T> type) {
		Object value = getPropertyValue(root, propertyPath);
		if (value != null) {
			Assert.isAssignable(type, value.getClass());
		}
		return (T) value;
	}

}
