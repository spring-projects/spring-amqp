/*
 * Copyright 2010-2012 the original author or authors.
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

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * @author Dave Syer
 * @author Gary Russell
 */
class TemplateParser extends AbstractSingleBeanDefinitionParser {

	private static final String CONNECTION_FACTORY_ATTRIBUTE = "connection-factory";

	private static final String EXCHANGE_ATTRIBUTE = "exchange";

	private static final String QUEUE_ATTRIBUTE = "queue";

	private static final String ROUTING_KEY_ATTRIBUTE = "routing-key";

	private static final String REPLY_TIMEOUT_ATTRIBUTE = "reply-timeout";

	private static final String MESSAGE_CONVERTER_ATTRIBUTE = "message-converter";

	private static final String ENCODING_ATTRIBUTE = "encoding";

	private static final String CHANNEL_TRANSACTED_ATTRIBUTE = "channel-transacted";

	private static final String REPLY_QUEUE = "reply-queue";

	@Override
	protected Class<?> getBeanClass(Element element) {
		return RabbitTemplate.class;
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

		if (!StringUtils.hasText(connectionFactoryRef)) {
			parserContext.getReaderContext().error("A '" + CONNECTION_FACTORY_ATTRIBUTE + "' attribute must be set.",
					element);
		}

		if (StringUtils.hasText(connectionFactoryRef)) {
			// Use constructor with connectionFactory parameter
			builder.addConstructorArgReference(connectionFactoryRef);
		}

		NamespaceUtils.setValueIfAttributeDefined(builder, element, CHANNEL_TRANSACTED_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, QUEUE_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, EXCHANGE_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, ROUTING_KEY_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, REPLY_TIMEOUT_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, ENCODING_ATTRIBUTE);
		NamespaceUtils.setReferenceIfAttributeDefined(builder, element, MESSAGE_CONVERTER_ATTRIBUTE);
		NamespaceUtils.setReferenceIfAttributeDefined(builder, element, REPLY_QUEUE);

	}

}
