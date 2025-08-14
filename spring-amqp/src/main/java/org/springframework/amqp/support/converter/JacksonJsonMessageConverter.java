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

import java.io.IOException;

import org.jspecify.annotations.Nullable;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeTypeUtils;

/**
 * JSON converter that uses the Jackson 3.
 *
 * @author Artem Bilan
 *
 * @since 4.0
 */
public class JacksonJsonMessageConverter extends AbstractJacksonMessageConverter {

	private @Nullable JacksonProjectingMessageConverter projectingConverter;

	/**
	 * Construct with an internal {@link JsonMapper} instance and trusted packed to all ({@code *}).
	 */
	public JacksonJsonMessageConverter() {
		this("*");
	}

	/**
	 * Construct with an internal {@link JsonMapper} instance.
	 * The {@link DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES}
	 * and {@link MapperFeature#DEFAULT_VIEW_INCLUSION} are set to false on the {@link JsonMapper}.
	 * @param trustedPackages the trusted Java packages for deserialization
	 * @see DefaultJacksonJavaTypeMapper#setTrustedPackages(String...)
	 */
	public JacksonJsonMessageConverter(String... trustedPackages) {
		this(JsonMapper.builder()
						.findAndAddModules(JacksonJsonMessageConverter.class.getClassLoader())
						.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
						.disable(MapperFeature.DEFAULT_VIEW_INCLUSION)
						.build(),
				trustedPackages);
	}

	/**
	 * Construct with the provided {@link JsonMapper} instance and trusted packed to all ({@code *}).
	 * @param jsonMapper the {@link JsonMapper} to use.
	 */
	public JacksonJsonMessageConverter(JsonMapper jsonMapper) {
		this(jsonMapper, "*");
	}

	/**
	 * Construct with the provided {@link JsonMapper} instance.
	 * @param jsonMapper the {@link JsonMapper} to use.
	 * @param trustedPackages the trusted Java packages for deserialization
	 * @see DefaultJacksonJavaTypeMapper#setTrustedPackages(String...)
	 */
	public JacksonJsonMessageConverter(JsonMapper jsonMapper, String... trustedPackages) {
		super(jsonMapper, MimeTypeUtils.parseMimeType(MessageProperties.CONTENT_TYPE_JSON), trustedPackages);
	}

	/**
	 * Set to true to use Spring Data projection to create the object if the inferred
	 * parameter type is an interface.
	 * @param useProjectionForInterfaces true to use projection.
	 */
	public void setUseProjectionForInterfaces(boolean useProjectionForInterfaces) {
		if (useProjectionForInterfaces) {
			if (!ClassUtils.isPresent("org.springframework.data.projection.ProjectionFactory", getClassLoader())) {
				throw new IllegalStateException("'spring-data-commons' is required to use Projection Interfaces");
			}
			this.projectingConverter = new JacksonProjectingMessageConverter((JsonMapper) this.objectMapper);
		}
	}

	protected boolean isUseProjectionForInterfaces() {
		return this.projectingConverter != null;
	}

	@Override
	protected Object convertContent(Message message, @Nullable Object conversionHint, MessageProperties properties,
			@Nullable String encoding) throws IOException {

		Object content = null;

		JavaType inferredType = getJavaTypeMapper().getInferredType(properties);
		if (inferredType != null && this.projectingConverter != null && inferredType.isInterface()
				&& !inferredType.getRawClass().getPackage().getName().startsWith("java.util")) { // List etc

			content = this.projectingConverter.convert(message, inferredType.getRawClass());
			properties.setProjectionUsed(true);
		}

		if (content == null) {
			return super.convertContent(message, conversionHint, properties, encoding);
		}
		else {
			return content;
		}
	}

}
