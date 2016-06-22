/*
 * Copyright 2002-2016 the original author or authors.
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

import org.codehaus.jackson.map.type.CollectionType;
import org.codehaus.jackson.map.type.MapType;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.ClassUtils;



/**
 * @author Mark Pollack
 * @author Sam Nelson
 * @author Andreas Asplund
 * @author Artem Bilan
 * @author Gary Russell
 */
public class DefaultJavaTypeMapper extends AbstractJavaTypeMapper implements JavaTypeMapper, ClassMapper {

	@Override
	public JavaType toJavaType(MessageProperties properties) {
		JavaType classType = getClassIdType(retrieveHeader(properties,
				getClassIdFieldName()));
		if (!classType.isContainerType() || classType.isArrayType()) {
			return classType;
		}

		JavaType contentClassType = getClassIdType(retrieveHeader(properties,
				getContentClassIdFieldName()));
		if (classType.getKeyType() == null) {
			return CollectionType.construct(
					classType.getRawClass(),
					contentClassType);
		}

		JavaType keyClassType = getClassIdType(retrieveHeader(properties,
				getKeyClassIdFieldName()));
		return MapType.construct(
				classType.getRawClass(), keyClassType,
				contentClassType);

	}

	private JavaType getClassIdType(String classId) {
		if (getIdClassMapping().containsKey(classId)) {
			return TypeFactory.defaultInstance().constructType(getIdClassMapping().get(classId));
		}

		try {
			return TypeFactory.defaultInstance().constructType(ClassUtils.forName(classId, getClassLoader()));
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

	@Override
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

	@Override
	public void fromClass(Class<?> clazz, MessageProperties properties) {
		fromJavaType(TypeFactory.defaultInstance().constructType(clazz), properties);

	}

	@Override
	public Class<?> toClass(MessageProperties properties) {
		return toJavaType(properties).getRawClass();
	}

}
