/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.support.converter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.type.TypeFactory;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Jackson 3 type mapper.
 *
 * @author Artem Bilan
 *
 * @since 4.0
 */
public class DefaultJacksonJavaTypeMapper implements JacksonJavaTypeMapper, BeanClassLoaderAware {

	private static final List<String> TRUSTED_PACKAGES =
			Arrays.asList(
					"java.util",
					"java.lang"
			);

	private final Set<String> trustedPackages = new LinkedHashSet<>(TRUSTED_PACKAGES);

	private volatile TypePrecedence typePrecedence = TypePrecedence.INFERRED;

	public static final String DEFAULT_CLASSID_FIELD_NAME = "__TypeId__";

	public static final String DEFAULT_CONTENT_CLASSID_FIELD_NAME = "__ContentTypeId__";

	public static final String DEFAULT_KEY_CLASSID_FIELD_NAME = "__KeyTypeId__";

	private final Map<String, Class<?>> idClassMapping = new HashMap<>();

	private final Map<Class<?>, String> classIdMapping = new HashMap<>();

	private @Nullable ClassLoader classLoader = ClassUtils.getDefaultClassLoader();

	private TypeFactory typeFactory = TypeFactory.createDefaultInstance();

	public String getClassIdFieldName() {
		return DEFAULT_CLASSID_FIELD_NAME;
	}

	public String getContentClassIdFieldName() {
		return DEFAULT_CONTENT_CLASSID_FIELD_NAME;
	}

	public String getKeyClassIdFieldName() {
		return DEFAULT_KEY_CLASSID_FIELD_NAME;
	}

	public void setIdClassMapping(Map<String, Class<?>> idClassMapping) {
		this.idClassMapping.putAll(idClassMapping);
		createReverseMap();
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
		this.typeFactory = this.typeFactory.withClassLoader(classLoader);
	}

	protected @Nullable ClassLoader getClassLoader() {
		return this.classLoader;
	}

	protected void addHeader(MessageProperties properties, String headerName, Class<?> clazz) {
		if (this.classIdMapping.containsKey(clazz)) {
			properties.getHeaders().put(headerName, this.classIdMapping.get(clazz));
		}
		else {
			properties.getHeaders().put(headerName, clazz.getName());
		}
	}

	protected String retrieveHeader(MessageProperties properties, String headerName) {
		String classId = retrieveHeaderAsString(properties, headerName);
		if (classId == null) {
			throw new MessageConversionException(
					"failed to convert Message content. Could not resolve " + headerName + " in header");
		}
		return classId;
	}

	protected @Nullable String retrieveHeaderAsString(MessageProperties properties, String headerName) {
		Map<String, @Nullable Object> headers = properties.getHeaders();
		Object classIdFieldNameValue = headers.get(headerName);
		return classIdFieldNameValue != null
				? classIdFieldNameValue.toString()
				: null;
	}

	private void createReverseMap() {
		this.classIdMapping.clear();
		for (Map.Entry<String, Class<?>> entry : this.idClassMapping.entrySet()) {
			String id = entry.getKey();
			Class<?> clazz = entry.getValue();
			this.classIdMapping.put(clazz, id);
		}
	}

	public Map<String, Class<?>> getIdClassMapping() {
		return Collections.unmodifiableMap(this.idClassMapping);
	}

	protected boolean hasInferredTypeHeader(MessageProperties properties) {
		return properties.getInferredArgumentType() != null;
	}

	protected JavaType fromInferredTypeHeader(MessageProperties properties) {
		return this.typeFactory.constructType(properties.getInferredArgumentType());
	}

	/**
	 * Return the precedence.
	 * @return the precedence.
	 * @since 1.6.
	 * @see #setTypePrecedence(TypePrecedence)
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
	 * set the precedence to {@link TypePrecedence#TYPE_ID}.
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
	public void setTrustedPackages(String @Nullable ... trustedPackages) {
		if (trustedPackages != null) {
			for (String trusted : trustedPackages) {
				if ("*".equals(trusted)) {
					this.trustedPackages.clear();
					break;
				}
				else {
					this.trustedPackages.add(trusted);
				}
			}
		}
	}

	@Override
	public void addTrustedPackages(String @Nullable ... packages) {
		setTrustedPackages(packages);
	}

	@Override
	public JavaType toJavaType(MessageProperties properties) {
		JavaType inferredType = getInferredType(properties);
		if (inferredType != null && canConvert(inferredType)) {
			return inferredType;
		}

		String typeIdHeader = retrieveHeaderAsString(properties, getClassIdFieldName());

		if (typeIdHeader != null) {
			return fromTypeHeader(properties, typeIdHeader);
		}

		if (hasInferredTypeHeader(properties)) {
			return fromInferredTypeHeader(properties);
		}

		return this.typeFactory.constructType(Object.class);
	}

	private boolean canConvert(JavaType inferredType) {
		if (inferredType.isAbstract() && !inferredType.isContainerType()) {
			return false;
		}
		if (inferredType.isContainerType() && inferredType.getContentType().isAbstract()) {
			return false;
		}
		return inferredType.getKeyType() == null || !inferredType.getKeyType().isAbstract();
	}

	private JavaType fromTypeHeader(MessageProperties properties, String typeIdHeader) {
		JavaType classType = getClassIdType(typeIdHeader);
		if (!classType.isContainerType() || classType.isArrayType()) {
			return classType;
		}

		JavaType contentClassType = getClassIdType(retrieveHeader(properties, getContentClassIdFieldName()));
		if (classType.getKeyType() == null) {
			return this.typeFactory.constructCollectionLikeType(classType.getRawClass(), contentClassType);
		}

		JavaType keyClassType = getClassIdType(retrieveHeader(properties, getKeyClassIdFieldName()));
		return this.typeFactory.constructMapLikeType(classType.getRawClass(), keyClassType, contentClassType);
	}

	@Override
	public @Nullable JavaType getInferredType(MessageProperties properties) {
		if (this.typePrecedence.equals(TypePrecedence.INFERRED) && hasInferredTypeHeader(properties)) {
			return fromInferredTypeHeader(properties);
		}
		return null;
	}

	private JavaType getClassIdType(String classId) {
		if (getIdClassMapping().containsKey(classId)) {
			return this.typeFactory.constructType(getIdClassMapping().get(classId));
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
					return this.typeFactory.constructType(ClassUtils.forName(classId, getClassLoader()));
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
		fromJavaType(this.typeFactory.constructType(clazz), properties);

	}

	@Override
	public Class<?> toClass(MessageProperties properties) {
		return toJavaType(properties).getRawClass();
	}

}
