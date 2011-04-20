/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

/**
 * @author Dave Syer
 *
 */
public class HeadersExchangeParser extends AbstractExchangeParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return HeadersExchange.class;
	}

	@Override
	protected AbstractBeanDefinition parseBinding(String exchangeName, Element binding, ParserContext parserContext) {
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(Binding.class);
		builder.addConstructorArgValue(new TypedStringValue(binding.getAttribute(BINDING_QUEUE_ATTR)));
		builder.addConstructorArgValue(DestinationType.EXCHANGE);
		builder.addConstructorArgValue(new TypedStringValue(exchangeName));
		builder.addConstructorArgValue("");
		ManagedMap<TypedStringValue, TypedStringValue> map = new ManagedMap<TypedStringValue, TypedStringValue>();
		String key = binding.getAttribute("key");
		String value = binding.getAttribute("value");
		map.put(new TypedStringValue(key), new TypedStringValue(value));
		builder.addConstructorArgValue(map);
		return builder.getBeanDefinition();
	}

}
