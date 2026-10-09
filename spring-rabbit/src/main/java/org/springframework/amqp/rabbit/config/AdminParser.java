/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import org.w3c.dom.Element;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;

/**
 * Parser for &lt;rabbit:admin/&gt;.
 * @author Tomas Lukosius
 * @since 1.0
 */
class AdminParser extends AbstractSingleBeanDefinitionParser {

	private static final String CONNECTION_FACTORY_ATTRIBUTE = "connection-factory";

	private static final String AUTO_STARTUP_ATTRIBUTE = "auto-startup";

	private static final String IGNORE_DECLARATION_EXCEPTIONS = "ignore-declaration-exceptions";

	@Override
	protected String getBeanClassName(Element element) {
		return "org.springframework.amqp.rabbit.core.RabbitAdmin";
	}

	@Override
	protected boolean shouldGenerateId() {
		return false;
	}

	@Override
	protected boolean shouldGenerateIdAsFallback() {
		return true;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		String connectionFactoryRef = element.getAttribute(CONNECTION_FACTORY_ATTRIBUTE);

		// At least one of 'templateRef' or 'connectionFactoryRef' attribute must be set.
		if (!StringUtils.hasText(connectionFactoryRef)) {
			parserContext.getReaderContext().error("A '" + CONNECTION_FACTORY_ATTRIBUTE + "' attribute must be set.",
					element);
		}

		if (StringUtils.hasText(connectionFactoryRef)) {
			// Use constructor with connectionFactory parameter
			builder.addConstructorArgReference(connectionFactoryRef);
		}

		String attributeValue;
		attributeValue = element.getAttribute(AUTO_STARTUP_ATTRIBUTE);
		if (StringUtils.hasText(attributeValue)) {
			builder.addPropertyValue("autoStartup", attributeValue);
		}

		NamespaceUtils.setValueIfAttributeDefined(builder, element, IGNORE_DECLARATION_EXCEPTIONS);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, "explicit-declarations-only");
	}

}
