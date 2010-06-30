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

import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Start of implementation for a potential XML based broker administration schema.
 * @author Mark Pollack
 *
 */
public class RabbitAdminParser extends AbstractBeanDefinitionParser {


	protected static final String EXCHANGES_ELEMENT = "exchanges";
	
	protected static final String DIRECT_EXCHANGE_ELEMENT = "direct-exchange";
	
	@Override
	protected boolean shouldGenerateId() {
		return false;
	}

	@Override
	protected boolean shouldGenerateIdAsFallback() {
		return true;
	}
	
	@Override
	protected AbstractBeanDefinition parseInternal(Element element,	ParserContext parserContext) {
		
		NodeList childNodes = element.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++) {
			Node child = childNodes.item(i);
			if (child.getNodeType() == Node.ELEMENT_NODE) {
				String localName = parserContext.getDelegate().getLocalName(child);
				if (EXCHANGES_ELEMENT.equals(localName)) {
					parseExchanges((Element) child, element, parserContext);
				}
			}
		}
		
		return null;
	}

	private void parseExchanges(Element exchangesElement, Element element,	ParserContext parserContext) {
		
		NodeList exchangeNodes = exchangesElement.getChildNodes();
		for (int i = 0; i < exchangeNodes.getLength(); i++) {
			Node exchange = exchangeNodes.item(i);
			if (exchange.getNodeType() == Node.ELEMENT_NODE) {
				String localName = parserContext.getDelegate().getLocalName(exchange);
				if (DIRECT_EXCHANGE_ELEMENT.equals(localName)) {
					parseDirectExchange((Element) exchange, exchangesElement, parserContext);
				}
			}
		}
		
	}

	private void parseDirectExchange(Element directExchangeElement, Element exchangesElement,
			ParserContext parserContext) {
		
		
	}

}
