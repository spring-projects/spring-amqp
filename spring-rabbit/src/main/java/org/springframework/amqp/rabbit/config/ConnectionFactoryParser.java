/*
 * Copyright 2010-2011 the original author or authors.
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

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

/**
 * @author Dave Syer
 * @author Gary Russell
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

		NamespaceUtils.addConstructorArgParentRefIfAttributeDefined(builder, element, CONNECTION_FACTORY_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, CHANNEL_CACHE_SIZE_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, HOST_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, PORT_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, USER_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, PASSWORD_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, VIRTUAL_HOST_ATTRIBUTE);
		NamespaceUtils.setValueIfAttributeDefined(builder, element, ADDRESSES);

	}

}
