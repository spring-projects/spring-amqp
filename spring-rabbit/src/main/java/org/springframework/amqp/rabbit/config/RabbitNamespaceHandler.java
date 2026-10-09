/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 * Namespace handler for Rabbit.
 *
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Gary Russell
 * @since 1.0
 */
public class RabbitNamespaceHandler extends NamespaceHandlerSupport {

	@Override
	public void init() {
		registerBeanDefinitionParser("queue", new QueueParser());
		registerBeanDefinitionParser("direct-exchange", new DirectExchangeParser());
		registerBeanDefinitionParser("topic-exchange", new TopicExchangeParser());
		registerBeanDefinitionParser("fanout-exchange", new FanoutExchangeParser());
		registerBeanDefinitionParser("headers-exchange", new HeadersExchangeParser());
		registerBeanDefinitionParser("listener-container", new ListenerContainerParser());
		registerBeanDefinitionParser("admin", new AdminParser());
		registerBeanDefinitionParser("connection-factory", new ConnectionFactoryParser());
		registerBeanDefinitionParser("template", new TemplateParser());
		registerBeanDefinitionParser("queue-arguments", new QueueArgumentsParser());
		registerBeanDefinitionParser("annotation-driven", new AnnotationDrivenParser());
	}

}
