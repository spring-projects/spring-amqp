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

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * @author tomas.lukosius@opencredo.com
 * @since 1.0
 */
class AdminParser extends AbstractSingleBeanDefinitionParser {

	private static final String TEMPLATE_ATTRIBUTE = "template";

	private static final String CONNECTION_FACTORY_ATTRIBUTE = "connection-factory";

	private static final String PHASE_ATTRIBUTE = "phase";
	
	private static final String AUTO_STARTUP_ATTRIBUTE = "auto-startup";

	@Override
	protected String getBeanClassName(Element element) {
		return "org.springframework.amqp.rabbit.core.RabbitAdmin";
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
		boolean templateAttributeExist = element.getAttributeNode(TEMPLATE_ATTRIBUTE) != null;
		boolean connectionFactoryAttributeExist = element.getAttributeNode(CONNECTION_FACTORY_ATTRIBUTE) != null;
		// Only one of 'templateRef' or 'connectionFactoryRef' can be set.
		if (templateAttributeExist && connectionFactoryAttributeExist) {
			parserContext.getReaderContext().error(
					"Either '" + TEMPLATE_ATTRIBUTE + "' or '" + CONNECTION_FACTORY_ATTRIBUTE
					+ "' attribute must be set.", element);
		}

		String templateRef = element.getAttribute(TEMPLATE_ATTRIBUTE);
		String connectionFactoryRef = element.getAttribute(CONNECTION_FACTORY_ATTRIBUTE);
		
		// At least one of 'templateRef' or 'connectionFactoryRef' attribute must be set.
		if (!StringUtils.hasText(templateRef) && !StringUtils.hasText(connectionFactoryRef)) {
			parserContext.getReaderContext().error(
					"One of '" + TEMPLATE_ATTRIBUTE + "' or '" + CONNECTION_FACTORY_ATTRIBUTE
							+ "' attribute must be set.", element);
		}


		if (StringUtils.hasText(templateRef)) {
			// Use constructor with template parameter
			builder.addConstructorArgReference(templateRef);
		} else if (StringUtils.hasText(connectionFactoryRef)) {
			// Use constructor with connectionFactory parameter
			builder.addConstructorArgReference(connectionFactoryRef);
		}

		String attributeValue;
		attributeValue = element.getAttribute(PHASE_ATTRIBUTE);
		if (StringUtils.hasText(attributeValue)) {
			builder.addPropertyValue("phase", attributeValue);
		}
		
		attributeValue = element.getAttribute(AUTO_STARTUP_ATTRIBUTE);
		if (StringUtils.hasText(attributeValue)) {
			builder.addPropertyValue("autoStartup", attributeValue);
		}
	}
}
