/*
 * Copyright 2002-2014 the original author or authors.
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
