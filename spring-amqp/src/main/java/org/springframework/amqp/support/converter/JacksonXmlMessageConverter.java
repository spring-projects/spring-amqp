/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.amqp.support.converter;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.dataformat.xml.XmlMapper;

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
	 * and no any trusted packed.
	 */
	public JacksonXmlMessageConverter() {
		this(new String[0]);
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
	 * and no any trusted packed.
	 * @param xmlMapper the {@link XmlMapper} to use.
	 */
	public JacksonXmlMessageConverter(XmlMapper xmlMapper) {
		this(xmlMapper, new String[0]);
	}

	/**
	 * Construct with the provided {@link XmlMapper} instance.
	 * @param xmlMapper the {@link XmlMapper} to use.
	 * @param trustedPackages the trusted Java packages for deserialization
	 * @see DefaultJacksonJavaTypeMapper#setTrustedPackages(String...)
	 */
	public JacksonXmlMessageConverter(XmlMapper xmlMapper, String... trustedPackages) {
		super(xmlMapper, MimeTypeUtils.parseMimeType("application/*+xml"), trustedPackages);
	}

}
