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

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Element;

import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.parsing.CompositeComponentDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;

/**
 * @author Mark Fisher
 * @author Gary Russell
 * @since 1.0
 */
class ListenerContainerParser implements BeanDefinitionParser {

	private static final String LISTENER_ELEMENT = "listener";

	private static final String ID_ATTRIBUTE = "id";

	private static final String GROUP_ATTRIBUTE = "group";

	private static final String QUEUE_NAMES_ATTRIBUTE = "queue-names";

	private static final String QUEUES_ATTRIBUTE = "queues";

	private static final String REF_ATTRIBUTE = "ref";

	private static final String METHOD_ATTRIBUTE = "method";

	private static final String MESSAGE_CONVERTER_ATTRIBUTE = "message-converter";

	private static final String RESPONSE_EXCHANGE_ATTRIBUTE = "response-exchange";

	private static final String RESPONSE_ROUTING_KEY_ATTRIBUTE = "response-routing-key";

	private static final String EXCLUSIVE = "exclusive";

	@SuppressWarnings("unchecked")
	@Override
	public BeanDefinition parse(Element element, ParserContext parserContext) {
		CompositeComponentDefinition compositeDef = new CompositeComponentDefinition(element.getTagName(),
				parserContext.extractSource(element));
		parserContext.pushContainingComponent(compositeDef);

		String group = element.getAttribute(GROUP_ATTRIBUTE);
		ManagedList<RuntimeBeanReference> containerList = null;
		if (StringUtils.hasText(group)) {
			BeanDefinition groupDef;
			if (parserContext.getRegistry().containsBeanDefinition(group)) {
				groupDef = parserContext.getRegistry().getBeanDefinition(group);
			}
			else {
				BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(ArrayList.class);
				builder.addConstructorArgValue(new ManagedList<RuntimeBeanReference>());
				groupDef = builder.getBeanDefinition();
				BeanDefinitionHolder holder = new BeanDefinitionHolder(groupDef, group);
				BeanDefinitionReaderUtils.registerBeanDefinition(holder, parserContext.getRegistry());
			}
			ConstructorArgumentValues constructorArgumentValues = groupDef.getConstructorArgumentValues();
			if (!ArrayList.class.getName().equals(groupDef.getBeanClassName())
					|| constructorArgumentValues.getArgumentCount() != 1
					|| constructorArgumentValues.getIndexedArgumentValue(0, ManagedList.class) == null) {
				parserContext.getReaderContext().error("Unexpected configuration for bean " + group, element);
			}
			containerList = (ManagedList<RuntimeBeanReference>) constructorArgumentValues
					.getIndexedArgumentValue(0, ManagedList.class).getValue();
		}

		List<Element> childElements = DomUtils.getChildElementsByTagName(element, LISTENER_ELEMENT);
		for (int i = 0; i < childElements.size(); i++) {
			parseListener(childElements.get(i), element, parserContext, containerList);
		}

		parserContext.popAndRegisterContainingComponent();
		return null;
	}

	private void parseListener(Element listenerEle, Element containerEle, ParserContext parserContext,
			ManagedList<RuntimeBeanReference> containerList) {
		RootBeanDefinition listenerDef = new RootBeanDefinition();
		listenerDef.setSource(parserContext.extractSource(listenerEle));

		String ref = listenerEle.getAttribute(REF_ATTRIBUTE);
		if (!StringUtils.hasText(ref)) {
			parserContext.getReaderContext().error("Listener 'ref' attribute contains empty value.", listenerEle);
		}
		else {
			listenerDef.getPropertyValues().add("delegate", new RuntimeBeanReference(ref));
		}

		String method = null;
		if (listenerEle.hasAttribute(METHOD_ATTRIBUTE)) {
			method = listenerEle.getAttribute(METHOD_ATTRIBUTE);
			if (!StringUtils.hasText(method)) {
				parserContext.getReaderContext()
						.error("Listener 'method' attribute contains empty value.", listenerEle);
			}
		}
		listenerDef.getPropertyValues().add("defaultListenerMethod", method);

		if (containerEle.hasAttribute(MESSAGE_CONVERTER_ATTRIBUTE)) {
			String messageConverter = containerEle.getAttribute(MESSAGE_CONVERTER_ATTRIBUTE);
			if (!StringUtils.hasText(messageConverter)) {
				parserContext.getReaderContext().error(
						"Listener container 'message-converter' attribute contains empty value.", containerEle);
			}
			else {
				listenerDef.getPropertyValues().add("messageConverter", new RuntimeBeanReference(messageConverter));
			}
		}

		BeanDefinition containerDef = RabbitNamespaceUtils.parseContainer(containerEle, parserContext);

		if (listenerEle.hasAttribute(RESPONSE_EXCHANGE_ATTRIBUTE)) {
			String responseExchange = listenerEle.getAttribute(RESPONSE_EXCHANGE_ATTRIBUTE);
			listenerDef.getPropertyValues().add("responseExchange", responseExchange);
		}

		if (listenerEle.hasAttribute(RESPONSE_ROUTING_KEY_ATTRIBUTE)) {
			String responseRoutingKey = listenerEle.getAttribute(RESPONSE_ROUTING_KEY_ATTRIBUTE);
			listenerDef.getPropertyValues().add("responseRoutingKey", responseRoutingKey);
		}

		listenerDef.setBeanClass(MessageListenerAdapter.class);
		containerDef.getPropertyValues().add("messageListener", listenerDef);

		String exclusive = listenerEle.getAttribute(EXCLUSIVE);
		if (StringUtils.hasText(exclusive)) {
			containerDef.getPropertyValues().add("exclusive", exclusive);
		}

		String childElementId = listenerEle.getAttribute(ID_ATTRIBUTE);
		String containerBeanName = StringUtils.hasText(childElementId) ? childElementId :
			BeanDefinitionReaderUtils.generateBeanName(containerDef, parserContext.getRegistry());

		if (!NamespaceUtils.isAttributeDefined(listenerEle, QUEUE_NAMES_ATTRIBUTE)
				&& !NamespaceUtils.isAttributeDefined(listenerEle, QUEUES_ATTRIBUTE)) {
			parserContext.getReaderContext().error("Listener 'queue-names' or 'queues' attribute must be provided.",
					listenerEle);
		}
		if (NamespaceUtils.isAttributeDefined(listenerEle, QUEUE_NAMES_ATTRIBUTE)
				&& NamespaceUtils.isAttributeDefined(listenerEle, QUEUES_ATTRIBUTE)) {
			parserContext.getReaderContext().error("Listener 'queue-names' or 'queues' attribute must be provided but not both.",
					listenerEle);
		}

		String queueNames = listenerEle.getAttribute(QUEUE_NAMES_ATTRIBUTE);
		if (StringUtils.hasText(queueNames)) {
			String[] names = StringUtils.commaDelimitedListToStringArray(queueNames);
			List<TypedStringValue> values = new ManagedList<TypedStringValue>();
			for (int i = 0; i < names.length; i++) {
				values.add(new TypedStringValue(names[i].trim()));
			}
			containerDef.getPropertyValues().add("queueNames", values);
		}
		String queues = listenerEle.getAttribute(QUEUES_ATTRIBUTE);
		if (StringUtils.hasText(queues)) {
			String[] names = StringUtils.commaDelimitedListToStringArray(queues);
			List<RuntimeBeanReference> values = new ManagedList<RuntimeBeanReference>();
			for (int i = 0; i < names.length; i++) {
				values.add(new RuntimeBeanReference(names[i].trim()));
			}
			containerDef.getPropertyValues().add("queues", values);
		}

		ManagedMap<String, TypedStringValue> args = new ManagedMap<String, TypedStringValue>();

		String priority = listenerEle.getAttribute("priority");
		if (StringUtils.hasText(priority)) {
			args.put("x-priority", new TypedStringValue(priority, Integer.class));
		}

		if (args.size() > 0) {
			containerDef.getPropertyValues().add("consumerArguments", args);
		}

		String admin = listenerEle.getAttribute("admin");
		if (StringUtils.hasText(admin)) {
			containerDef.getPropertyValues().add("rabbitAdmin", new RuntimeBeanReference(admin));
		}

		// Register the listener and fire event
		parserContext.registerBeanComponent(new BeanComponentDefinition(containerDef, containerBeanName));

		if (containerList != null) {
			containerList.add(new RuntimeBeanReference(containerBeanName));
		}
	}

}
