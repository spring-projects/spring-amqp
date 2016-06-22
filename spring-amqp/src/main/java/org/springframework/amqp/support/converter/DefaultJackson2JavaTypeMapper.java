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

import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * @author Mark Pollack
 * @author Sam Nelson
 * @author Andreas Asplund
 * @author Artem Bilan
 * @author Gary Russell
 */
public class DefaultJackson2JavaTypeMapper extends AbstractJavaTypeMapper
		implements Jackson2JavaTypeMapper, ClassMapper {

	private volatile TypePrecedence typePrecedence = TypePrecedence.INFERRED;

	/**
	 * Return the precedence.
	 * @return the precedence.
	 * @see #setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence)
	 * @since 1.6.
	 */
	@Override
	public TypePrecedence getTypePrecedence() {
		return this.typePrecedence;
	}

	/**
	 * Set the precedence for evaluating type information in message properties.
	 * When using {@code @RabbitListener} at the method level, the framework attempts
	 * to determine the target type for payload conversion from the method signature.
	 * If so, this type is provided in the
	 * {@link MessageProperties#getInferredArgumentType() inferredArgumentType}
	 * message property.
	 * <p>
	 * By default, if the type is concrete (not abstract, not an interface), this will
	 * be used ahead of type information provided in the {@code __TypeId__} and
	 * associated headers provided by the sender.
	 * <p>
	 * If you wish to force the use of the  {@code __TypeId__} and associated headers
	 * (such as when the actual type is a subclass of the method argument type),
	 * set the precedence to {@link Jackson2JavaTypeMapper.TypePrecedence#TYPE_ID}.
	 *
	 * @param typePrecedence the precedence.
	 * @since 1.6
	 */
	public void setTypePrecedence(TypePrecedence typePrecedence) {
		Assert.notNull(typePrecedence, "'typePrecedence' cannot be null");
		this.typePrecedence = typePrecedence;
	}

	@Override
	public JavaType toJavaType(MessageProperties properties) {
		boolean hasInferredTypeHeader = hasInferredTypeHeader(properties);
		if (hasInferredTypeHeader && this.typePrecedence.equals(TypePrecedence.INFERRED)) {
			JavaType targetType = fromInferredTypeHeader(properties);
			if ((!targetType.isAbstract() && !targetType.isInterface())
					|| targetType.getRawClass().getPackage().getName().startsWith("java.util")) {
				return targetType;
			}
		}

		String typeIdHeader = retrieveHeaderAsString(properties, getClassIdFieldName());

		if (typeIdHeader != null) {

			JavaType classType = getClassIdType(typeIdHeader);
			if (!classType.isContainerType() || classType.isArrayType()) {
				return classType;
			}

			JavaType contentClassType = getClassIdType(retrieveHeader(properties, getContentClassIdFieldName()));
			if (classType.getKeyType() == null) {
				return CollectionType.construct(classType.getRawClass(), contentClassType);
			}

			JavaType keyClassType = getClassIdType(retrieveHeader(properties, getKeyClassIdFieldName()));
			return MapType.construct(classType.getRawClass(), keyClassType, contentClassType);
		}

		if (hasInferredTypeHeader) {
			return fromInferredTypeHeader(properties);
		}

		return TypeFactory.defaultInstance().constructType(Object.class);
	}

	private JavaType getClassIdType(String classId) {
		if (getIdClassMapping().containsKey(classId)) {
			return TypeFactory.defaultInstance().constructType(getIdClassMapping().get(classId));
		}

		try {
			return TypeFactory.defaultInstance()
					.constructType(ClassUtils.forName(classId, getClassLoader()));
		}
		catch (ClassNotFoundException e) {
			throw new MessageConversionException("failed to resolve class name. Class not found [" + classId + "]", e);
		}
		catch (LinkageError e) {
			throw new MessageConversionException("failed to resolve class name. Linkage error [" + classId + "]", e);
		}
	}

	@Override
	public void fromJavaType(JavaType javaType, MessageProperties properties) {
		addHeader(properties, getClassIdFieldName(), javaType.getRawClass());

		if (javaType.isContainerType() && !javaType.isArrayType()) {
			addHeader(properties, getContentClassIdFieldName(), javaType.getContentType().getRawClass());
		}

		if (javaType.getKeyType() != null) {
			addHeader(properties, getKeyClassIdFieldName(), javaType.getKeyType().getRawClass());
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
