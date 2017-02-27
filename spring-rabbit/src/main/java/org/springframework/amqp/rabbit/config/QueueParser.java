/*
 * Copyright 2002-2017 the original author or authors.
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

import java.util.Map;

import org.w3c.dom.Element;

import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;

/**
 * @author Dave Syer
 * @author Gary Russell
 * @author Felipe Gutierrez
 * @author Artem Bilan
 *
 */
public class QueueParser extends AbstractSingleBeanDefinitionParser {

	private static final ThreadLocal<Element> CURRENT_ELEMENT = new ThreadLocal<>();

	/**  Element OR attribute. */
	private static final String ARGUMENTS = "queue-arguments";

	private static final String DURABLE_ATTRIBUTE = "durable";

	private static final String EXCLUSIVE_ATTRIBUTE = "exclusive";

	private static final String AUTO_DELETE_ATTRIBUTE = "auto-delete";

	private static final String REF_ATTRIBUTE = "ref";

	private static final String NAMING_STRATEGY = "naming-strategy";

	@Override
	protected boolean shouldGenerateIdAsFallback() {
		return true;
	}

	@Override
	protected boolean shouldParseNameAsAliases() {
		Element element = CURRENT_ELEMENT.get();
		try {
			return element == null || !element.hasAttribute(ID_ATTRIBUTE);
		}
		finally {
			CURRENT_ELEMENT.remove();
		}
	}

	@Override
	protected Class<?> getBeanClass(Element element) {
		if (NamespaceUtils.isAttributeDefined(element, NAME_ATTRIBUTE)) {
			return Queue.class;
		}
		else {
			return AnonymousQueue.class;
		}
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		if (!NamespaceUtils.isAttributeDefined(element, NAME_ATTRIBUTE)
				&& !NamespaceUtils.isAttributeDefined(element, ID_ATTRIBUTE)) {
			parserContext.getReaderContext().error("Queue must have either id or name (or both)", element);
		}

		NamespaceUtils.addConstructorArgValueIfAttributeDefined(builder, element, NAME_ATTRIBUTE);

		if (!NamespaceUtils.isAttributeDefined(element, NAME_ATTRIBUTE)) {

			if (attributeHasIllegalOverride(element, DURABLE_ATTRIBUTE, "false")
					|| attributeHasIllegalOverride(element, EXCLUSIVE_ATTRIBUTE, "true")
					|| attributeHasIllegalOverride(element, AUTO_DELETE_ATTRIBUTE, "true")) {
				parserContext.getReaderContext().error(
						"Anonymous queue cannot specify durable='true', exclusive='false' or auto-delete='false'",
						element);
			}
			NamespaceUtils.addConstructorArgRefIfAttributeDefined(builder, element, NAMING_STRATEGY);

		}
		else {
			if (StringUtils.hasText(element.getAttribute(NAMING_STRATEGY))) {
				parserContext.getReaderContext().error("Only one of 'name' or 'naming-strategy' is allowed", element);
			}
			NamespaceUtils.addConstructorArgBooleanValueIfAttributeDefined(builder, element, DURABLE_ATTRIBUTE, false);
			NamespaceUtils
					.addConstructorArgBooleanValueIfAttributeDefined(builder, element, EXCLUSIVE_ATTRIBUTE, false);
			NamespaceUtils.addConstructorArgBooleanValueIfAttributeDefined(builder, element, AUTO_DELETE_ATTRIBUTE,
					false);

		}

		String queueArguments = element.getAttribute(ARGUMENTS);
		Element argumentsElement = DomUtils.getChildElementByTagName(element, ARGUMENTS);

		if (argumentsElement != null) {
			if (StringUtils.hasText(queueArguments)) {
				parserContext
						.getReaderContext()
						.error("Queue may have either a queue-attributes attribute or element, but not both",
								element);
			}

			String ref = argumentsElement.getAttribute(REF_ATTRIBUTE);
			Map<?, ?> map = parserContext.getDelegate().parseMapElement(argumentsElement,
					builder.getRawBeanDefinition());
			if (StringUtils.hasText(ref)) {
				if (map != null && map.size() > 0) {
					parserContext.getReaderContext()
							.error("You cannot have both a 'ref' and a nested map", element);
				}
				builder.addConstructorArgReference(ref);
			}
			else {
				builder.addConstructorArgValue(map);
			}
		}

		if (StringUtils.hasText(queueArguments)) {
			builder.addConstructorArgReference(queueArguments);
		}

		NamespaceUtils.parseDeclarationControls(element, builder);
		CURRENT_ELEMENT.set(element);
	}

	private boolean attributeHasIllegalOverride(Element element, String name, String allowed) {
		return element.getAttributeNode(name) != null && element.getAttributeNode(name).getSpecified()
				&& !allowed.equals(element.getAttribute(name));
	}

}
