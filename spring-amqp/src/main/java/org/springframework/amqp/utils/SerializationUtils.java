/*
 * Copyright 2006-present the original author or authors.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.Set;

import org.springframework.core.ConfigurableObjectInputStream;
import org.springframework.util.ObjectUtils;
import org.springframework.util.PatternMatchUtils;

/**
 * Static utility to help with serialization.
 *
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 */
public final class SerializationUtils {

	private static final String TRUST_ALL_ENV = "SPRING_AMQP_DESERIALIZATION_TRUST_ALL";

	private static final String TRUST_ALL_PROP = "spring.amqp.deserialization.trust.all";

	private static final boolean TRUST_ALL;

	static {
		TRUST_ALL = Boolean.parseBoolean(System.getenv(TRUST_ALL_ENV))
				|| Boolean.parseBoolean(System.getProperty(TRUST_ALL_PROP));
	}

	private SerializationUtils() {
	}

	/**
	 * Serialize the object provided.
	 *
	 * @param object the object to serialize
	 * @return an array of bytes representing the object in a portable fashion
	 */
	public static byte[] serialize(Object object) {
		if (object == null) {
			return null;
		}
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		try {
			new ObjectOutputStream(stream).writeObject(object);
		}
		catch (IOException e) {
			throw new IllegalArgumentException("Could not serialize object of type: " + object.getClass(), e);
		}
		return stream.toByteArray();
	}

	/**
	 * Deserialize the bytes.
	 * @param bytes a serialized object created
	 * @return the result of deserializing the bytes
	 */
	public static Object deserialize(byte[] bytes) {
		if (bytes == null) {
			return null;
		}
		try {
			return deserialize(new ObjectInputStream(new ByteArrayInputStream(bytes)));
		}
		catch (IOException e) {
			throw new IllegalArgumentException("Could not deserialize object", e);
		}
	}

	/**
	 * Deserialize the stream.
	 * @param stream an object stream created from a serialized object
	 * @return the result of deserializing the bytes
	 */
	public static Object deserialize(ObjectInputStream stream) {
		if (stream == null) {
			return null;
		}
		try {
			return stream.readObject();
		}
		catch (IOException e) {
			throw new IllegalArgumentException("Could not deserialize object", e);
		}
		catch (ClassNotFoundException e) {
			throw new IllegalStateException("Could not deserialize object type", e);
		}
	}

	/**
	 * Deserialize the stream.
	 * @param inputStream the stream.
	 * @param allowedListPatterns allowed classes.
	 * @param classLoader the class loader.
	 * @return the result.
	 * @throws IOException IO Exception.
	 * @since 2.1
	 */
	public static Object deserialize(InputStream inputStream, Set<String> allowedListPatterns, ClassLoader classLoader)
			throws IOException {

		try (
				ObjectInputStream objectInputStream = new ConfigurableObjectInputStream(inputStream, classLoader) {

					@Override
					protected Class<?> resolveClass(ObjectStreamClass classDesc)
							throws IOException, ClassNotFoundException {
						Class<?> clazz = super.resolveClass(classDesc);
						checkAllowedList(clazz, allowedListPatterns);
						return clazz;
					}

				}) {

			return objectInputStream.readObject();
		}
		catch (ClassNotFoundException ex) {
			throw new IOException("Failed to deserialize object type", ex);
		}
	}

	/**
	 * Verify that the class is in the allowed list.
	 * @param clazz the class.
	 * @param patterns the patterns.
	 * @since 2.1
	 */
	public static void checkAllowedList(Class<?> clazz, Set<String> patterns) {
		if (TRUST_ALL && ObjectUtils.isEmpty(patterns)) {
			return;
		}
		if (clazz.isArray() || clazz.isPrimitive() || Number.class.isAssignableFrom(clazz)
				|| String.class.equals(clazz)) {
			return;
		}
		String className = clazz.getName();
		for (String pattern : patterns) {
			if (PatternMatchUtils.simpleMatch(pattern, className)) {
				return;
			}
		}
		throw new SecurityException("Attempt to deserialize unauthorized " + clazz
				+ "; add allowed class name patterns to the message converter or, if you trust the message originator, "
				+ "set environment variable '"
				+ TRUST_ALL_ENV + "' or system property '" + TRUST_ALL_PROP + "' to true");
	}

}
