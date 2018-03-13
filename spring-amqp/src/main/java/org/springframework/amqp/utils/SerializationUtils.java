/*
 * Copyright 2006-2018 the original author or authors.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.Set;

import org.springframework.core.ConfigurableObjectInputStream;
import org.springframework.core.NestedIOException;
import org.springframework.util.ObjectUtils;
import org.springframework.util.PatternMatchUtils;

/**
 * Static utility to help with serialization.
 *
 * @author Dave Syer
 * @author Gary Russell
 */
public final class SerializationUtils {

	private SerializationUtils() {
		super();
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
	 * @param whiteListPatterns allowed classes.
	 * @param classLoader the class loader.
	 * @return the result.
	 * @throws IOException IO Exception.
	 * @since 2.1
	 */
	public static Object deserialize(InputStream inputStream, Set<String> whiteListPatterns, ClassLoader classLoader)
			throws IOException {

		try (
			ObjectInputStream objectInputStream = new ConfigurableObjectInputStream(inputStream, classLoader) {

				@Override
				protected Class<?> resolveClass(ObjectStreamClass classDesc)
						throws IOException, ClassNotFoundException {
					Class<?> clazz = super.resolveClass(classDesc);
					checkWhiteList(clazz, whiteListPatterns);
					return clazz;
				}

			}) {

			return objectInputStream.readObject();
		}
		catch (ClassNotFoundException ex) {
			throw new NestedIOException("Failed to deserialize object type", ex);
		}
	}

	/**
	 * Verify that the class is in the white list.
	 * @param clazz the class.
	 * @param whiteListPatterns the patterns.
	 * @since 2.1
	 */
	public static void checkWhiteList(Class<?> clazz, Set<String> whiteListPatterns) {
		if (ObjectUtils.isEmpty(whiteListPatterns)) {
			return;
		}
		if (clazz.isArray() || clazz.isPrimitive() || clazz.equals(String.class)
				|| Number.class.isAssignableFrom(clazz)) {
			return;
		}
		String className = clazz.getName();
		for (String pattern : whiteListPatterns) {
			if (PatternMatchUtils.simpleMatch(pattern, className)) {
				return;
			}
		}
		throw new SecurityException("Attempt to deserialize unauthorized " + clazz);
	}

}
