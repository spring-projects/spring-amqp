/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import org.w3c.dom.Element;

import org.springframework.amqp.core.FanoutExchange;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;

/**
 * @author Dave Syer
 * @author Gary Russell
 *
 */
public class FanoutExchangeParser extends AbstractExchangeParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return FanoutExchange.class;
	}

	@Override
	protected BeanDefinitionBuilder parseBinding(String exchangeName, Element binding, ParserContext parserContext) {
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(BindingFactoryBean.class);
		parseDestination(binding, parserContext, builder);
		builder.addPropertyValue("exchange", new TypedStringValue(exchangeName));
		return builder;
	}

}
