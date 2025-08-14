/*
 * Copyright 2018-present the original author or authors.
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
import tools.jackson.dataformat.xml.XmlMapper;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.util.MimeTypeUtils;

/**
 * XML converter that uses the Jackson 3 XML mapper.
 *
 * @author Artem Bilan
 *
 * @since 4.0
 */
public class JacksonXmlMessageConverter extends AbstractJacksonMessageConverter {

	/**
	 * Construct with an internal {@link XmlMapper} instance
	 * and trusted packed to all ({@code *}).
	 */
	public JacksonXmlMessageConverter() {
		this("*");
	}

	/**
	 * Construct with an internal {@link XmlMapper} instance.
	 * The {@link DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES} is set to false on
	 * the {@link XmlMapper}.
	 * @param trustedPackages the trusted Java packages for deserialization
	 * @see DefaultJacksonJavaTypeMapper#setTrustedPackages(String...)
	 */
	public JacksonXmlMessageConverter(String... trustedPackages) {
		this(XmlMapper.xmlBuilder()
						.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
						.build(),
				trustedPackages);
	}

	/**
	 * Construct with the provided {@link XmlMapper} instance
	 * and trusted packed to all ({@code *}).
	 * @param xmlMapper the {@link XmlMapper} to use.
	 */
	public JacksonXmlMessageConverter(XmlMapper xmlMapper) {
		this(xmlMapper, "*");
	}

	/**
	 * Construct with the provided {@link XmlMapper} instance.
	 * @param xmlMapper the {@link XmlMapper} to use.
	 * @param trustedPackages the trusted Java packages for deserialization
	 * @see DefaultJacksonJavaTypeMapper#setTrustedPackages(String...)
	 */
	public JacksonXmlMessageConverter(XmlMapper xmlMapper, String... trustedPackages) {
		super(xmlMapper, MimeTypeUtils.parseMimeType(MessageProperties.CONTENT_TYPE_XML), trustedPackages);
	}

}
