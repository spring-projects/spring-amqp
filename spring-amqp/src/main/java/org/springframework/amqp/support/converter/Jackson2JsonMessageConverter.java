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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.MimeTypeUtils;

/**
 * JSON converter that uses the Jackson 2 Json library.
 *
 * @author Mark Pollack
 * @author James Carr
 * @author Dave Syer
 * @author Sam Nelson
 * @author Andreas Asplund
 * @author Gary Russell
 * @author Artem Bilan
 * @author Arlo Louis O'Keeffe
 * @author Mohammad Hewedy
 *
 * @deprecated since 4.0 in favor of {@link JacksonJsonMessageConverter} for Jackson 3.
 */
@Deprecated(forRemoval = true, since = "4.0")
public class Jackson2JsonMessageConverter extends AbstractJackson2MessageConverter {

	/**
	 * Construct with an internal {@link ObjectMapper} instance
	 * and trusted packed to all ({@code *}).
	 * @since 1.6.11
	 * @see JacksonUtils#enhancedObjectMapper()
	 */
	public Jackson2JsonMessageConverter() {
		this("*");
	}

	/**
	 * Construct with an internal {@link ObjectMapper} instance.
	 * The {@link DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES} is set to false on
	 * the {@link ObjectMapper}.
	 * @param trustedPackages the trusted Java packages for deserialization
	 * @since 1.6.11
	 * @see DefaultJackson2JavaTypeMapper#setTrustedPackages(String...)
	 * @see JacksonUtils#enhancedObjectMapper()
	 */
	public Jackson2JsonMessageConverter(String... trustedPackages) {
		this(JacksonUtils.enhancedObjectMapper(), trustedPackages);
	}

	/**
	 * Construct with the provided {@link ObjectMapper} instance
	 * and trusted packed to all ({@code *}).
	 * @param jsonObjectMapper the {@link ObjectMapper} to use.
	 * @since 1.6.12
	 */
	public Jackson2JsonMessageConverter(ObjectMapper jsonObjectMapper) {
		this(jsonObjectMapper, "*");
	}

	/**
	 * Construct with the provided {@link ObjectMapper} instance.
	 * @param jsonObjectMapper the {@link ObjectMapper} to use.
	 * @param trustedPackages the trusted Java packages for deserialization
	 * @since 1.6.11
	 * @see DefaultJackson2JavaTypeMapper#setTrustedPackages(String...)
	 */
	public Jackson2JsonMessageConverter(ObjectMapper jsonObjectMapper, String... trustedPackages) {
		super(jsonObjectMapper, MimeTypeUtils.parseMimeType(MessageProperties.CONTENT_TYPE_JSON), trustedPackages);
	}

}
