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

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.MimeTypeUtils;

/**
 * JSON converter that uses the Jackson 3.
 *
 * @author Artem Bilan
 *
 * @since 4.0
 */
public class JacksonJsonMessageConverter extends AbstractJacksonMessageConverter {

	/**
	 * Construct with an internal {@link ObjectMapper} instance and trusted packed to all ({@code *}).
	 */
	public JacksonJsonMessageConverter() {
		this("*");
	}

	/**
	 * Construct with an internal {@link ObjectMapper} instance.
	 * The {@link DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES}
	 * and {@link MapperFeature#DEFAULT_VIEW_INCLUSION} are set to false on the {@link ObjectMapper}.
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
	 * Construct with the provided {@link ObjectMapper} instance and trusted packed to all ({@code *}).
	 * @param jsonObjectMapper the {@link ObjectMapper} to use.
	 */
	public JacksonJsonMessageConverter(ObjectMapper jsonObjectMapper) {
		this(jsonObjectMapper, "*");
	}

	/**
	 * Construct with the provided {@link ObjectMapper} instance.
	 * @param jsonObjectMapper the {@link ObjectMapper} to use.
	 * @param trustedPackages the trusted Java packages for deserialization
	 * @see DefaultJacksonJavaTypeMapper#setTrustedPackages(String...)
	 */
	public JacksonJsonMessageConverter(ObjectMapper jsonObjectMapper, String... trustedPackages) {
		super(jsonObjectMapper, MimeTypeUtils.parseMimeType(MessageProperties.CONTENT_TYPE_JSON), trustedPackages);
	}

}
