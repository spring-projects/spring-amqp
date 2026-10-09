/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import org.w3c.dom.Element;

import org.springframework.amqp.core.DirectExchange;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 */
public class DirectExchangeParser extends AbstractExchangeParser {

	private static final String BINDING_KEY_ATTR = "key";

	@Override
	protected Class<?> getBeanClass(Element element) {
		return DirectExchange.class;
	}

	@Override
	protected BeanDefinitionBuilder parseBinding(String exchangeName, Element binding,
												 ParserContext parserContext) {
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(BindingFactoryBean.class);
		parseDestination(binding, parserContext, builder);
		builder.addPropertyValue("exchange", new TypedStringValue(exchangeName));
		String queueId = binding.getAttribute(BINDING_QUEUE_ATTR);
		String exchangeId = binding.getAttribute(BINDING_EXCHANGE_ATTR);

		String bindingKey = binding.hasAttribute(BINDING_KEY_ATTR) ? binding.getAttribute(BINDING_KEY_ATTR) :
				"#{@'" + (StringUtils.hasText(queueId) ? queueId : exchangeId)  + "'.name}";

		builder.addPropertyValue("routingKey", new TypedStringValue(bindingKey));
		return builder;
	}

}
