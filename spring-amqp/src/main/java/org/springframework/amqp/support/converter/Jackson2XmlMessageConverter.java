/*
 * Copyright 2018-2025 the original author or authors.
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
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.MimeTypeUtils;

/**
 * XML converter that uses the Jackson 2 Xml library.
 *
 * @author Mohammad Hewedy
 *
 * @since 2.1
 */
public class Jackson2XmlMessageConverter extends AbstractJackson2MessageConverter {

	/**
	 * Construct with an internal {@link XmlMapper} instance
	 * and trusted packed to all ({@code *}).
	 */
	public Jackson2XmlMessageConverter() {
		this("*");
	}

	/**
	 * Construct with an internal {@link XmlMapper} instance.
	 * The {@link DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES} is set to false on
	 * the {@link XmlMapper}.
	 * @param trustedPackages the trusted Java packages for deserialization
	 * @see DefaultJackson2JavaTypeMapper#setTrustedPackages(String...)
	 */
	public Jackson2XmlMessageConverter(String... trustedPackages) {
		this(new XmlMapper(), trustedPackages);
		this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	/**
	 * Construct with the provided {@link XmlMapper} instance
	 * and trusted packed to all ({@code *}).
	 * @param xmlMapper the {@link XmlMapper} to use.
	 */
	public Jackson2XmlMessageConverter(XmlMapper xmlMapper) {
		this(xmlMapper, "*");
	}

	/**
	 * Construct with the provided {@link XmlMapper} instance.
	 * @param xmlMapper the {@link XmlMapper} to use.
	 * @param trustedPackages the trusted Java packages for deserialization
	 * @see DefaultJackson2JavaTypeMapper#setTrustedPackages(String...)
	 */
	public Jackson2XmlMessageConverter(XmlMapper xmlMapper, String... trustedPackages) {
		super(xmlMapper, MimeTypeUtils.parseMimeType(MessageProperties.CONTENT_TYPE_XML), trustedPackages);
	}

}
