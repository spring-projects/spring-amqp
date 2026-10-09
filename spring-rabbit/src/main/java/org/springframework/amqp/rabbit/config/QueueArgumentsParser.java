/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import java.util.Map;

import org.w3c.dom.Element;

import org.springframework.beans.factory.config.MapFactoryBean;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;

/**
 * @author Gary Russell
 * @since 1.0.1
 *
 */
class QueueArgumentsParser extends AbstractSingleBeanDefinitionParser {

	@Override
	protected void doParse(Element element, ParserContext parserContext,
			BeanDefinitionBuilder builder) {
		Map<?, ?> map = parserContext.getDelegate().parseMapElement(element,
				builder.getRawBeanDefinition());
		builder.addPropertyValue("sourceMap", map);
	}

	@Override
	protected String getBeanClassName(Element element) {
		return MapFactoryBean.class.getName();
	}

}
