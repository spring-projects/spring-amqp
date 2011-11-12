/*
 * Copyright 2002-2010 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.springframework.amqp.support.converter;

/**
 * 
 * @author Mark Pollack
 * @author Sam Nelson
 * 
 */
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.ClassUtils;

public class DefaultJavaTypeMapper implements JavaTypeMapper, InitializingBean {

	public static final String DEFAULT_CLASSID_FIELD_NAME = "__TypeId__";
	public static final String DEFAULT_CONTENT_CLASSID_FIELD_NAME = "__ContentTypeId__";
	private Map<String, Class<?>> idClassMapping = new HashMap<String, Class<?>>();
	private Map<Class<?>, String> classIdMapping = new HashMap<Class<?>, String>();

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public JavaType toJavaType(MessageProperties properties) {
		JavaType classType = getClassIdType(retrieveHeader(properties,
				getClassIdFieldName()));
		if (!classType.isContainerType()) {
			return classType;
		}

		String contentClassId = retrieveHeader(properties,
				getContentClassIdFieldName());
		JavaType contentClassType = getClassIdType(contentClassId);
		JavaType collectionType = TypeFactory.collectionType(
				(Class<? extends Collection>) classType.getRawClass(),
				contentClassType);
		return collectionType;

	}

	private JavaType getClassIdType(String classId) {
		if (this.idClassMapping.containsKey(classId)) {
			return TypeFactory.type(idClassMapping.get(classId));
		}

		try {
			return TypeFactory.type(ClassUtils.forName(classId, getClass()
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

	public String getClassIdFieldName() {
		return DEFAULT_CLASSID_FIELD_NAME;
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
	}

	private void addHeader(MessageProperties properties, String headerName,
			Class<?> clazz) {
		if (classIdMapping.containsKey(clazz)) {
			properties.getHeaders().put(headerName, classIdMapping.get(clazz));
		} else {
			properties.getHeaders().put(headerName, clazz.getName());
		}
	}

	public void afterPropertiesSet() throws Exception {
		validateIdTypeMapping();
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

	public String getContentClassIdFieldName() {
		return DEFAULT_CONTENT_CLASSID_FIELD_NAME;
	}

}
