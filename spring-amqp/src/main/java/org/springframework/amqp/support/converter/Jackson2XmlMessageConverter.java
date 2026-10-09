/*
 * Copyright 2018-present the original author or authors.
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
 *
 * @deprecated since 4.0 in favor of {@link JacksonXmlMessageConverter} for Jackson 3.
 */
@Deprecated(forRemoval = true, since = "4.0")
@SuppressWarnings("removal")
public class Jackson2XmlMessageConverter extends AbstractJackson2MessageConverter {

	/**
	 * Construct with an internal {@link XmlMapper} instance
	 * and no any trusted packages.
	 */
	public Jackson2XmlMessageConverter() {
		this(new String[0]);
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
	 * and no any trusted packages.
	 * @param xmlMapper the {@link XmlMapper} to use.
	 */
	public Jackson2XmlMessageConverter(XmlMapper xmlMapper) {
		this(xmlMapper, new String[0]);
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
