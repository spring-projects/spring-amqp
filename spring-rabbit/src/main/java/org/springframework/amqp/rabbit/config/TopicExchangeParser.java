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
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.BeanMetadataElement;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

/**
 * @author Dave Syer
 *
 */
public class TopicExchangeParser extends AbstractExchangeParser {

	private static final String BINDING_PATTERN_ATTR = "pattern";

	@Override
	protected Class<?> getBeanClass(Element element) {
		return TopicExchange.class;
	}

	@Override
	protected AbstractBeanDefinition parseBinding(BeanMetadataElement exchange, Element binding, ParserContext parserContext) {
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(Binding.class);
		builder.addConstructorArgReference(binding.getAttribute(BINDING_QUEUE_ATTR));
		builder.addConstructorArgValue(exchange);
		builder.addConstructorArgValue(binding.getAttribute(BINDING_PATTERN_ATTR));
		return builder.getBeanDefinition();
	}

}
