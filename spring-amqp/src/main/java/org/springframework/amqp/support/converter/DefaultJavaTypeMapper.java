/*
 * Copyright 2002-2013 the original author or authors. Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */
package org.springframework.amqp.support.converter;

import static org.codehaus.jackson.map.type.TypeFactory.collectionType;
import static org.codehaus.jackson.map.type.TypeFactory.mapType;
import static org.codehaus.jackson.map.type.TypeFactory.type;

import java.util.Collection;
import java.util.Map;

import org.codehaus.jackson.type.JavaType;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.ClassUtils;

/**
 * @author Mark Pollack
 * @author Sam Nelson
 * @author Andreas Asplund
 * @author Artem Bilan
 */
public class DefaultJavaTypeMapper extends AbstractJavaTypeMapper implements JavaTypeMapper, ClassMapper {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public JavaType toJavaType(MessageProperties properties) {
		JavaType classType = getClassIdType(retrieveHeader(properties,
				getClassIdFieldName()));
		if (!classType.isContainerType() || classType.isArrayType()) {
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
		return mapType(
				(Class<? extends Map>) classType.getRawClass(), keyClassType,
				contentClassType);

	}

	private JavaType getClassIdType(String classId) {
		if (getIdClassMapping().containsKey(classId)) {
			return type(getIdClassMapping().get(classId));
		}

		try {
			return type(ClassUtils.forName(classId, getClass()
					.getClassLoader()));
		}
		catch (ClassNotFoundException e) {
			throw new MessageConversionException(
					"failed to resolve class name. Class not found [" + classId
							+ "]", e);
		}
		catch (LinkageError e) {
			throw new MessageConversionException(
					"failed to resolve class name. Linkage error [" + classId
							+ "]", e);
		}
	}

	public void fromJavaType(JavaType javaType, MessageProperties properties) {
		addHeader(properties, getClassIdFieldName(),
				javaType.getRawClass());

		if (javaType.isContainerType() && !javaType.isArrayType()) {
			addHeader(properties, getContentClassIdFieldName(), javaType
					.getContentType().getRawClass());
		}

		if (javaType.getKeyType() != null) {
			addHeader(properties, getKeyClassIdFieldName(), javaType
					.getKeyType().getRawClass());
		}
	}

	public void fromClass(Class<?> clazz, MessageProperties properties) {
		fromJavaType(type(clazz), properties);

	}

	public Class<?> toClass(MessageProperties properties) {
		return toJavaType(properties).getRawClass();
	}

}
