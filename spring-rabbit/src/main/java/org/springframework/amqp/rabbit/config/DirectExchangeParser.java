/*
 * Copyright 2002-2010 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.amqp.rabbit.config;

import java.util.Collections;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * @author Dave Syer
 * 
 */
public class DirectExchangeParser extends AbstractExchangeParser {

	private static final String BINDING_KEY_ATTR = "key";

	@Override
	protected Class<?> getBeanClass(Element element) {
		return DirectExchange.class;
	}

	@Override
	protected AbstractBeanDefinition parseBinding(String exchangeName, Element binding,
			ParserContext parserContext) {
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(Binding.class);
		builder.addConstructorArgValue(binding.getAttribute(BINDING_QUEUE_ATTR));
		builder.addConstructorArgValue(DestinationType.EXCHANGE);
		builder.addConstructorArgValue(exchangeName);
		String bindingKey = binding.getAttribute(BINDING_KEY_ATTR);
		if (!StringUtils.hasText(bindingKey)) {
			bindingKey = "";
		}
		builder.addConstructorArgValue(bindingKey);
		builder.addConstructorArgValue(Collections.<String, Object>emptyMap());
		return builder.getBeanDefinition();
	}

}
