/*
 * Copyright 2002-2016 the original author or authors.
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

import java.util.List;

import org.w3c.dom.Element;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.core.Conventions;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 */
class TemplateParser extends AbstractSingleBeanDefinitionParser {

	private static final String CONNECTION_FACTORY_ATTRIBUTE = "connection-factory";

	private static final String EXCHANGE_ATTRIBUTE = "exchange";

	private static final String QUEUE_ATTRIBUTE = "queue";

	private static final String ROUTING_KEY_ATTRIBUTE = "routing-key";

	private static final String RECEIVE_TIMEOUT_ATTRIBUTE = "receive-timeout";

	private static final String REPLY_TIMEOUT_ATTRIBUTE = "reply-timeout";

	private static final String MESSAGE_CONVERTER_ATTRIBUTE = "message-converter";

	private static final String ENCODING_ATTRIBUTE = "encoding";

	private static final String CHANNEL_TRANSACTED_ATTRIBUTE = "channel-transacted";

	private static final String REPLY_QUEUE_ATTRIBUTE = "reply-queue";

	private static final String REPLY_ADDRESS_ATTRIBUTE = "reply-address";

	private static final String USE_TEMPORARY_REPLY_QUEUES_ATTRIBUTE = "use-temporary-reply-queues";

	private static final String LISTENER_ELEMENT = "reply-listener";

	private static final String MANDATORY_ATTRIBUTE = "mandatory";

	private static final String RETURN_CALLBACK_ATTRIBUTE = "return-callback";

	private static final String CONFIRM_CALLBACK_ATTRIBUTE = "confirm-callback";

	private static final String CORRELATION_KEY = "correlation-key";

	private static final String RETRY_TEMPLATE = "retry-template";

	private static final String RECOVERY_CALLBACK = "recovery-callback";

	private static final String DIRECT_REPLY_TO_CONTAINER = "direct-reply-to-container";

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
		NamespaceUtils.setValueIfAttributeDefined(builder, element, RECEIVE_TIMEOUT_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, REPLY_TIMEOUT_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, ENCODING_ATTRIBUTE);
		NamespaceUtils.setReferenceIfAttributeDefined(builder, element, MESSAGE_CONVERTER_ATTRIBUTE);
		String replyAddress = element.getAttribute(REPLY_ADDRESS_ATTRIBUTE);
		if (!StringUtils.hasText(replyAddress)) {
			NamespaceUtils.setReferenceIfAttributeDefined(builder, element, REPLY_QUEUE_ATTRIBUTE,
					Conventions.attributeNameToPropertyName(REPLY_ADDRESS_ATTRIBUTE));
		}
		NamespaceUtils.setValueIfAttributeDefined(builder, element, USE_TEMPORARY_REPLY_QUEUES_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, REPLY_ADDRESS_ATTRIBUTE);
		NamespaceUtils.setReferenceIfAttributeDefined(builder, element, RETURN_CALLBACK_ATTRIBUTE);
		NamespaceUtils.setReferenceIfAttributeDefined(builder, element, CONFIRM_CALLBACK_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, CORRELATION_KEY);
		NamespaceUtils.setReferenceIfAttributeDefined(builder, element, RETRY_TEMPLATE);
		NamespaceUtils.setReferenceIfAttributeDefined(builder, element, RECOVERY_CALLBACK);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, DIRECT_REPLY_TO_CONTAINER,
				"useDirectReplyToContainer");

		BeanDefinition expressionDef =
				NamespaceUtils.createExpressionDefinitionFromValueOrExpression(MANDATORY_ATTRIBUTE,
						"mandatory-expression", parserContext, element, false);
		if (expressionDef != null) {
			builder.addPropertyValue("mandatoryExpression", expressionDef);
		}

		BeanDefinition sendConnectionFactorySelectorExpression =
				NamespaceUtils.createExpressionDefIfAttributeDefined("send-connection-factory-selector-expression",
						element);
		if (sendConnectionFactorySelectorExpression != null) {
			builder.addPropertyValue("sendConnectionFactorySelectorExpression", sendConnectionFactorySelectorExpression);
		}

		BeanDefinition receiveConnectionFactorySelectorExpression =
				NamespaceUtils.createExpressionDefIfAttributeDefined("receive-connection-factory-selector-expression",
						element);
		if (receiveConnectionFactorySelectorExpression != null) {
			builder.addPropertyValue("receiveConnectionFactorySelectorExpression",
					receiveConnectionFactorySelectorExpression);
		}

		BeanDefinition userIdExpression = NamespaceUtils.createExpressionDefIfAttributeDefined("user-id-expression",
				element);
		if (userIdExpression != null) {
			builder.addPropertyValue("userIdExpression", userIdExpression);
		}

		BeanDefinition replyContainer = null;
		Element childElement = null;
		List<Element> childElements = DomUtils.getChildElementsByTagName(element, LISTENER_ELEMENT);
		if (childElements.size() > 0) {
			childElement = childElements.get(0);
		}
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

	private BeanDefinition parseListener(Element childElement, Element element,
			ParserContext parserContext) {
		BeanDefinition replyContainer = RabbitNamespaceUtils.parseContainer(childElement, parserContext);
		if (replyContainer != null) {
			replyContainer.getPropertyValues().add(
					"connectionFactory",
					new RuntimeBeanReference(element.getAttribute(CONNECTION_FACTORY_ATTRIBUTE)));
		}
		if (element.hasAttribute(REPLY_QUEUE_ATTRIBUTE)) {
			replyContainer.getPropertyValues().add("queues",
					new RuntimeBeanReference(element.getAttribute(REPLY_QUEUE_ATTRIBUTE)));
		}
		return replyContainer;
	}

}
