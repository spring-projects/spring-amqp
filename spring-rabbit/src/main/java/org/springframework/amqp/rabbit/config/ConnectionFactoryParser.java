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

import org.w3c.dom.Element;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Artem Bilan
 */
class ConnectionFactoryParser extends AbstractSingleBeanDefinitionParser {

	private static final String CONNECTION_FACTORY_ATTRIBUTE = "connection-factory";

	private static final String CHANNEL_CACHE_SIZE_ATTRIBUTE = "channel-cache-size";

	private static final String HOST_ATTRIBUTE = "host";

	private static final String PORT_ATTRIBUTE = "port";

	private static final String ADDRESSES = "addresses";

	private static final String VIRTUAL_HOST_ATTRIBUTE = "virtual-host";

	private static final String USER_ATTRIBUTE = "username";

	private static final String PASSWORD_ATTRIBUTE = "password";

	private static final String EXECUTOR_ATTRIBUTE = "executor";

	private static final String PUBLISHER_CONFIRMS = "publisher-confirms";

	private static final String PUBLISHER_RETURNS = "publisher-returns";

	private static final String REQUESTED_HEARTBEAT = "requested-heartbeat";

	private static final String CONNECTION_TIMEOUT = "connection-timeout";

	private static final String CACHE_MODE = "cache-mode";

	private static final String CONNECTION_CACHE_SIZE_ATTRIBUTE = "connection-cache-size";

	private static final String THREAD_FACTORY = "thread-factory";

	private static final String FACTORY_TIMEOUT = "factory-timeout";

	private static final String CONNECTION_LIMIT = "connection-limit";

	@Override
	protected Class<?> getBeanClass(Element element) {
		return CachingConnectionFactory.class;
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
		if (element.hasAttribute(ADDRESSES) &&
				(element.hasAttribute(HOST_ATTRIBUTE) || element.hasAttribute(PORT_ATTRIBUTE))) {
			parserContext.getReaderContext().error("If the 'addresses' attribute is provided, a connection " +
					"factory can not have 'host' or 'port' attributes.", element);
		}

		NamespaceUtils.addConstructorArgParentRefIfAttributeDefined(builder, element, CONNECTION_FACTORY_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, CHANNEL_CACHE_SIZE_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, HOST_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, PORT_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, USER_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, PASSWORD_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, VIRTUAL_HOST_ATTRIBUTE);
		NamespaceUtils.setReferenceIfAttributeDefined(builder, element, EXECUTOR_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, ADDRESSES);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, PUBLISHER_CONFIRMS);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, PUBLISHER_RETURNS);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, REQUESTED_HEARTBEAT, "requestedHeartBeat");
		NamespaceUtils.setValueIfAttributeDefined(builder, element, CONNECTION_TIMEOUT);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, CACHE_MODE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, CONNECTION_CACHE_SIZE_ATTRIBUTE);
		NamespaceUtils.setReferenceIfAttributeDefined(builder, element, THREAD_FACTORY, "connectionThreadFactory");
		NamespaceUtils.setValueIfAttributeDefined(builder, element, FACTORY_TIMEOUT, "channelCheckoutTimeout");
		NamespaceUtils.setValueIfAttributeDefined(builder, element, CONNECTION_LIMIT);
		NamespaceUtils.setReferenceIfAttributeDefined(builder, element, "connection-name-strategy");
	}

}
