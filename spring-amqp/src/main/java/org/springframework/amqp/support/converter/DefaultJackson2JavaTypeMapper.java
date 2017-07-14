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

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * Jackson 2 type mapper.
 * @author Mark Pollack
 * @author Sam Nelson
 * @author Andreas Asplund
 * @author Artem Bilan
 * @author Gary Russell
 */
public class DefaultJackson2JavaTypeMapper extends AbstractJavaTypeMapper
		implements Jackson2JavaTypeMapper, ClassMapper {

	private static final List<String> TRUSTED_PACKAGES =
			Arrays.asList(
					"java.util",
					"java.lang"
			);

	private final Set<String> trustedPackages = new LinkedHashSet<String>(TRUSTED_PACKAGES);

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

	/**
	 * Specify a set of packages to trust during deserialization.
	 * The asterisk ({@code *}) means trust all.
	 * @param trustedPackages the trusted Java packages for deserialization
	 * @since 1.6.11
	 */
	public void setTrustedPackages(String... trustedPackages) {
		if (trustedPackages != null) {
			for (String whiteListClass : trustedPackages) {
				if ("*".equals(whiteListClass)) {
					this.trustedPackages.clear();
					break;
				}
				else {
					this.trustedPackages.add(whiteListClass);
				}
			}
		}
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
				return TypeFactory.defaultInstance()
						.constructCollectionLikeType(classType.getRawClass(), contentClassType);
			}

			JavaType keyClassType = getClassIdType(retrieveHeader(properties, getKeyClassIdFieldName()));
			return TypeFactory.defaultInstance()
					.constructMapLikeType(classType.getRawClass(), keyClassType, contentClassType);
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
		else {
			try {
				if (!isTrustedPackage(classId)) {
					throw new IllegalArgumentException("The class '" + classId + "' is not in the trusted packages: " +
							this.trustedPackages + ". " +
							"If you believe this class is safe to deserialize, please provide its name. " +
							"If the serialization is only done by a trusted source, you can also enable trust all (*).");
				}
				else {
					return TypeFactory.defaultInstance()
							.constructType(ClassUtils.forName(classId, getClassLoader()));
				}
			}
			catch (ClassNotFoundException e) {
				throw new MessageConversionException("failed to resolve class name. Class not found [" + classId + "]", e);
			}
			catch (LinkageError e) {
				throw new MessageConversionException("failed to resolve class name. Linkage error [" + classId + "]", e);
			}
		}
	}

	private boolean isTrustedPackage(String requestedType) {
		if (!this.trustedPackages.isEmpty()) {
			String packageName = ClassUtils.getPackageName(requestedType).replaceFirst("\\[L", "");
			for (String trustedPackage : this.trustedPackages) {
				if (packageName.equals(trustedPackage)) {
					return true;
				}
			}
			return false;
		}
		return true;
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
