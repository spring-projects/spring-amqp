/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import org.w3c.dom.Element;

import org.springframework.amqp.core.HeadersExchange;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 * @author Ngoc Nhan
 *
 */
public class HeadersExchangeParser extends AbstractExchangeParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return HeadersExchange.class;
	}

	@Override
	protected BeanDefinitionBuilder parseBinding(String exchangeName, Element binding, ParserContext parserContext) {
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(BindingFactoryBean.class);
		Element argumentsElement = DomUtils.getChildElementByTagName(binding, BINDING_ARGUMENTS);
		String key = binding.getAttribute("key");
		String value = binding.getAttribute("value");
		boolean hasKey = StringUtils.hasText(key);
		boolean hasValue = StringUtils.hasText(value);

		if (argumentsElement != null && (hasKey || hasValue)) {
			parserContext.getReaderContext()
					.error("'binding-arguments' sub-element and 'key/value' attributes are mutually exclusive.", binding);
		}
		parseDestination(binding, parserContext, builder);

		if (hasKey ^ hasValue) {
			parserContext.getReaderContext().error("Both 'key/value' attributes have to be declared.", binding);
		}

		if (argumentsElement == null) {
			if (!hasKey & !hasValue) {
				parserContext.getReaderContext()
						.error("At least one of 'binding-arguments' sub-element or 'key/value' attributes pair have to be declared.", binding);
			}
			ManagedMap<TypedStringValue, TypedStringValue> map = new ManagedMap<>();
			map.put(new TypedStringValue(key), new TypedStringValue(value));
			builder.addPropertyValue("arguments", map);
		}

		builder.addPropertyValue("exchange", new TypedStringValue(exchangeName));
		return builder;
	}

}
