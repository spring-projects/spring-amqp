/*
 * Copyright 2002-2017 the original author or authors.
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

package org.springframework.amqp.support.converter;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.ClassUtils;

/**
 * Maps to/from JSON using type information in the {@link MessageProperties}; the default
 * name of the message property containing the type is
 * {@value #DEFAULT_CLASSID_FIELD_NAME}. An optional property
 * {@link #setDefaultType(Class)} is provided that allows mapping to a statically defined
 * type, if no message property is found in the message properties.
 * {@link #setIdClassMapping(Map)} can be used to map tokens in the
 * {@value #DEFAULT_CLASSID_FIELD_NAME} header to classes. If this class is not a
 * Spring-managed bean, call {@link #afterPropertiesSet()} to set up the class to id
 * mapping.
 * @author Mark Pollack
 * @author Gary Russell
 *
 */
public class DefaultClassMapper implements ClassMapper, InitializingBean {

	public static final String DEFAULT_CLASSID_FIELD_NAME = "__TypeId__";

	private static final String DEFAULT_HASHTABLE_TYPE_ID = "Hashtable";

	private volatile Map<String, Class<?>> idClassMapping = new HashMap<String, Class<?>>();

	private volatile Map<Class<?>, String> classIdMapping = new HashMap<Class<?>, String>();

	private volatile Class<?> defaultMapClass = LinkedHashMap.class;

	private volatile Class<?> defaultType = LinkedHashMap.class;

	/**
	 * The type returned by {@link #toClass(MessageProperties)} if no type information
	 * is found in the message properties.
	 * @param defaultType the defaultType to set.
	 */
	public void setDefaultType(Class<?> defaultType) {
		this.defaultType = defaultType;
	}

	/**
	 * Set the type of {@link Map} to use. For outbound messages, set the
	 * {@value #DEFAULT_CLASSID_FIELD_NAME} header to {@code Hashtable}. For inbound messages,
	 * if the {@value #DEFAULT_CLASSID_FIELD_NAME} header is {@code HashTable} convert to this
	 * class.
	 * @param defaultMapClass the map class.
	 * @deprecated use {@link #setDefaultMapClass(Class)}
	 */
	@Deprecated
	public void setDefaultHashtableClass(Class<?> defaultMapClass) {
		this.defaultMapClass = defaultMapClass;
	}

	/**
	 * Set the type of {@link Map} to use. For outbound messages, set the
	 * {@value #DEFAULT_CLASSID_FIELD_NAME} header to {@code HashTable}. For inbound messages,
	 * if the {@value #DEFAULT_CLASSID_FIELD_NAME} header is {@code HashTable} convert to this
	 * class.
	 * @param defaultMapClass the map class.
	 * @see #DEFAULT_CLASSID_FIELD_NAME
	 * @since 2.0
	 */
	public void setDefaultMapClass(Class<?> defaultMapClass) {
		this.defaultMapClass = defaultMapClass;
	}

	/**
	 * The name of the header that contains the type id.
	 * @return {@value #DEFAULT_CLASSID_FIELD_NAME}
	 * @see #DEFAULT_CLASSID_FIELD_NAME
	 */
	public String getClassIdFieldName() {
		return DEFAULT_CLASSID_FIELD_NAME;
	}

	/**
	 * Set a map of type Ids (in the {@value #DEFAULT_CLASSID_FIELD_NAME} header) to
	 * classes. For outbound messages, if the class is not in this map, the
	 * {@value #DEFAULT_CLASSID_FIELD_NAME} header is set to the fully qualified
	 * class name.
	 * @param idClassMapping the map of IDs to classes.
	 */
	public void setIdClassMapping(Map<String, Class<?>> idClassMapping) {
		this.idClassMapping = idClassMapping;
	}

	private String fromClass(Class<?> classOfObjectToConvert) {
		if (this.classIdMapping.containsKey(classOfObjectToConvert)) {
			return this.classIdMapping.get(classOfObjectToConvert);
		}
		if (Map.class.isAssignableFrom(classOfObjectToConvert)) {
			return DEFAULT_HASHTABLE_TYPE_ID;
		}
		return classOfObjectToConvert.getName();
	}

	private Class<?> toClass(String classId) {
		if (this.idClassMapping.containsKey(classId)) {
			return this.idClassMapping.get(classId);
		}
		if (classId.equals(DEFAULT_HASHTABLE_TYPE_ID)) {
			return this.defaultMapClass;
		}
		try {
			return ClassUtils.forName(classId, getClass().getClassLoader());
		}
		catch (ClassNotFoundException e) {
			throw new MessageConversionException(
					"failed to resolve class name [" + classId + "]", e);
		}
		catch (LinkageError e) {
			throw new MessageConversionException(
					"failed to resolve class name [" + classId + "]", e);
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>Creates the reverse mapping from class to type id.
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		validateIdTypeMapping();
	}

	private void validateIdTypeMapping() {
		Map<String, Class<?>> finalIdClassMapping = new HashMap<String, Class<?>>();
		this.classIdMapping.clear();
		for (Entry<String, Class<?>> entry : this.idClassMapping.entrySet()) {
			String id = entry.getKey();
			Class<?> clazz = entry.getValue();
			finalIdClassMapping.put(id, clazz);
			this.classIdMapping.put(clazz, id);
		}
		this.idClassMapping = finalIdClassMapping;
	}

	@Override
	public void fromClass(Class<?> clazz, MessageProperties properties) {
		properties.getHeaders().put(getClassIdFieldName(), fromClass(clazz));
	}

	@Override
	public Class<?> toClass(MessageProperties properties) {
		Map<String, Object> headers = properties.getHeaders();
		Object classIdFieldNameValue = headers.get(getClassIdFieldName());
		String classId = null;
		if (classIdFieldNameValue != null) {
			classId = classIdFieldNameValue.toString();
		}
		if (classId == null) {
			if (this.defaultType != null) {
				return this.defaultType;
			}
			else {
				throw new MessageConversionException(
						"failed to convert Message content. Could not resolve "
								+ getClassIdFieldName() + " in header " +
								"and no defaultType provided");
			}
		}
		return toClass(classId);
	}

}
