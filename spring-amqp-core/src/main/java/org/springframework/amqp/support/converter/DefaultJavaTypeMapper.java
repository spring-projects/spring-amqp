/*
 * Copyright 2002-2010 the original author or authors. Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */
package org.springframework.amqp.support.converter;

/**
 * @author Mark Pollack
 * @author Sam Nelson
 */
import static org.codehaus.jackson.map.type.TypeFactory.collectionType;
import static org.codehaus.jackson.map.type.TypeFactory.mapType;
import static org.codehaus.jackson.map.type.TypeFactory.type;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.type.JavaType;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.ClassUtils;

public class DefaultJavaTypeMapper implements JavaTypeMapper, ClassMapper,
		InitializingBean {

	public static final String DEFAULT_CLASSID_FIELD_NAME = "__TypeId__";
	public static final String DEFAULT_CONTENT_CLASSID_FIELD_NAME = "__ContentTypeId__";
	public static final String DEFAULT_KEY_CLASSID_FIELD_NAME = "__KeyTypeId__";

	private Map<String, Class<?>> idClassMapping = new HashMap<String, Class<?>>();
	private Map<Class<?>, String> classIdMapping = new HashMap<Class<?>, String>();

	public String getClassIdFieldName() {
		return DEFAULT_CLASSID_FIELD_NAME;
	}

	public String getContentClassIdFieldName() {
		return DEFAULT_CONTENT_CLASSID_FIELD_NAME;
	}

	public String getKeyClassIdFieldName() {
		return DEFAULT_KEY_CLASSID_FIELD_NAME;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public JavaType toJavaType(MessageProperties properties) {
		JavaType classType = getClassIdType(retrieveHeader(properties,
				getClassIdFieldName()));
		if (!classType.isContainerType()) {
			return classType;
		}

		JavaType contentClassType = getClassIdType(retrieveHeader(properties,
				getContentClassIdFieldName()));
		if (classType.getKeyType() == null) {
			return collectionType(
					(Class<? extends Collection>) classType.getRawClass(),
					contentClassType);
		}

		JavaType keyClassType = getClassIdType(retrieveHeader(properties,
				getKeyClassIdFieldName()));
		JavaType mapType = mapType(
				(Class<? extends Map>) classType.getRawClass(), keyClassType,
				contentClassType);
		return mapType;

	}

	private JavaType getClassIdType(String classId) {
		if (this.idClassMapping.containsKey(classId)) {
			return type(idClassMapping.get(classId));
		}

		try {
			return type(ClassUtils.forName(classId, getClass()
					.getClassLoader()));
		} catch (ClassNotFoundException e) {
			throw new MessageConversionException(
					"failed to resolve class name. Class not found [" + classId
							+ "]", e);
		} catch (LinkageError e) {
			throw new MessageConversionException(
					"failed to resolve class name. Linkage error [" + classId
							+ "]", e);
		}
	}

	private String retrieveHeader(MessageProperties properties,
			String headerName) {
		Map<String, Object> headers = properties.getHeaders();
		Object classIdFieldNameValue = headers.get(headerName);
		String classId = null;
		if (classIdFieldNameValue != null) {
			classId = classIdFieldNameValue.toString();
		}
		if (classId == null) {
			throw new MessageConversionException(
					"failed to convert Message content. Could not resolve "
							+ headerName + " in header");
		}
		return classId;
	}

	public void setIdClassMapping(Map<String, Class<?>> idClassMapping) {
		this.idClassMapping = idClassMapping;
	}

	public void fromJavaType(JavaType javaType, MessageProperties properties) {
		addHeader(properties, getClassIdFieldName(),
				(Class<?>) javaType.getRawClass());

		if (javaType.isContainerType()) {
			addHeader(properties, getContentClassIdFieldName(), javaType
					.getContentType().getRawClass());
		}

		if (javaType.getKeyType() != null) {
			addHeader(properties, getKeyClassIdFieldName(), javaType
					.getKeyType().getRawClass());
		}
	}

	public void afterPropertiesSet() throws Exception {
		validateIdTypeMapping();
	}

	private void addHeader(MessageProperties properties, String headerName,
			Class<?> clazz) {
		if (classIdMapping.containsKey(clazz)) {
			properties.getHeaders().put(headerName, classIdMapping.get(clazz));
		} else {
			properties.getHeaders().put(headerName, clazz.getName());
		}
	}

	private void validateIdTypeMapping() {
		Map<String, Class<?>> finalIdClassMapping = new HashMap<String, Class<?>>();
		for (Entry<String, Class<?>> entry : idClassMapping.entrySet()) {
			String id = entry.getKey();
			Class<?> clazz = entry.getValue();
			finalIdClassMapping.put(id, clazz);
			classIdMapping.put(clazz, id);
		}
		this.idClassMapping = finalIdClassMapping;
	}

	public void fromClass(Class<?> clazz, MessageProperties properties) {
		fromJavaType(type(clazz), properties);

	}

	public Class<?> toClass(MessageProperties properties) {
		return toJavaType(properties).getRawClass();
	}
}
