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
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

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

	private static final String REPLY_QUEUE_ATTRIBUTE = "reply-queue";

	private static final String LISTENER_ELEMENT = "reply-listener";

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
		return false;
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
		NamespaceUtils.setReferenceIfAttributeDefined(builder, element, REPLY_QUEUE_ATTRIBUTE);

		BeanDefinition replyContainer = null;
		Element childElement = getChildElement(element, parserContext);
		if (childElement != null) {
			replyContainer = parseListener(childElement, element,
					parserContext);
			if (replyContainer != null) {
				replyContainer.getPropertyValues().add("messageListener",
						new RuntimeBeanReference(element.getAttribute(ID_ATTRIBUTE)));
				String replyContainerName = element.getAttribute(ID_ATTRIBUTE) + ".replyListener";
				parserContext.getRegistry().registerBeanDefinition(replyContainerName, replyContainer);
			}
		}
		if (replyContainer == null && element.hasAttribute(REPLY_QUEUE_ATTRIBUTE)) {
			parserContext.getReaderContext().error(
					"For template '" + element.getAttribute(ID_ATTRIBUTE)
							+ "', when specifying a reply-queue, "
							+ "a <reply-listener/> element is required",
					element);
		}
		else if (replyContainer != null && !element.hasAttribute(REPLY_QUEUE_ATTRIBUTE)) {
			parserContext.getReaderContext().error(
					"For template '" + element.getAttribute(ID_ATTRIBUTE)
							+ "', a <reply-listener/> element is not allowed if no " +
							"'reply-queue' is supplied",
					element);
		}
	}

	private Element getChildElement(Element element,
			ParserContext parserContext) {
		Element childElement = null;
		NodeList childNodes = element.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			Node child = childNodes.item(i);
			if (child.getNodeType() == Node.ELEMENT_NODE) {
				String localName = parserContext.getDelegate().getLocalName(child);
				if (LISTENER_ELEMENT.equals(localName)) {
					childElement = (Element) child;
				}
			}
		}
		return childElement;
	}

	private BeanDefinition parseListener(Element childElement, Element element,
			ParserContext parserContext) {
		if (getChildElement(childElement, parserContext) != null) {
			parserContext.getReaderContext().error("<reply-listener/> is not allowed any child elements.", element);
		}
		BeanDefinition replyContainer = RabbitNamespaceUtils.parseContainer(childElement, parserContext);
		if (replyContainer != null) {
			replyContainer.getPropertyValues().add(
					"connectionFactory",
					new RuntimeBeanReference(element.getAttribute(CONNECTION_FACTORY_ATTRIBUTE)));
		}
		replyContainer.getPropertyValues().add("queues", element.getAttribute(REPLY_QUEUE_ATTRIBUTE));
		return replyContainer;
	}

}
