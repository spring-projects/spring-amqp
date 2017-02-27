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

import java.util.List;

import org.w3c.dom.Element;

import org.springframework.amqp.rabbit.support.ExpressionFactoryBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.BeanReference;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.xml.BeanDefinitionParserDelegate;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.core.Conventions;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;

/**
 * Shared utility methods for namespace parsers.
 * @author Mark Pollack
 * @author Dave Syer
 * @author Gary Russell
 *
 */
public abstract class NamespaceUtils {

	public static final String BASE_PACKAGE = "org.springframework.amqp.core.rabbit.config";
	public static final String REF_ATTRIBUTE = "ref";
	public static final String METHOD_ATTRIBUTE = "method";
	public static final String ORDER = "order";

	/**
	 * Populates the specified bean definition property with the value of the attribute whose name is provided if that
	 * attribute is defined in the given element.
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be used to populate the property
	 * @param propertyName the name of the property to be populated
	 *
	 * @return true if defined.
	 */
	public static boolean setValueIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName, String propertyName) {
		String attributeValue = element.getAttribute(attributeName);
		if (StringUtils.hasText(attributeValue)) {
			builder.addPropertyValue(propertyName, new TypedStringValue(attributeValue));
			return true;
		}
		return false;
	}

	/**
	 * Populates the bean definition property corresponding to the specified attributeName with the value of that
	 * attribute if it is defined in the given element.
	 *
	 * <p>
	 * The property name will be the camel-case equivalent of the lower case hyphen separated attribute (e.g. the
	 * "foo-bar" attribute would match the "fooBar" property).
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be set on the property
	 * @return true if defined.
	 * @see Conventions#attributeNameToPropertyName(String)
	 */
	public static boolean setValueIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName) {
		return setValueIfAttributeDefined(builder, element, attributeName,
				Conventions.attributeNameToPropertyName(attributeName));
	}

	/**
	 * Checks the attribute to see if it is defined in the given element.
	 *
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be used as a constructor argument
	 * @return true if defined.
	 */
	public static boolean isAttributeDefined(Element element, String attributeName) {
		String value = element.getAttribute(attributeName);
		return (StringUtils.hasText(value));
	}

	/**
	 * Populates the bean definition constructor argument with the value of that attribute if it is defined in the given
	 * element.
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be used as a constructor argument
	 * @return true if defined.
	 */
	public static boolean addConstructorArgValueIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName) {
		String value = element.getAttribute(attributeName);
		if (StringUtils.hasText(value)) {
			builder.addConstructorArgValue(new TypedStringValue(value));
			return true;
		}
		return false;
	}

	/**
	 * Populates the bean definition constructor argument with the boolean value of that attribute if it is defined in
	 * the given element or else uses the default provided.
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be used as a constructor argument
	 * @param defaultValue the default value to use if the attirbute is not set
	 */
	public static void addConstructorArgBooleanValueIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName, boolean defaultValue) {
		String value = element.getAttribute(attributeName);
		if (StringUtils.hasText(value)) {
			builder.addConstructorArgValue(new TypedStringValue(value));
		}
		else {
			builder.addConstructorArgValue(defaultValue);
		}
	}

	/**
	 * Populates the bean definition constructor argument with a reference to a bean with id equal to the attribute if
	 * it is defined in the given element.
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be used to set the reference
	 *
	 * @return true if defined.
	 */
	public static boolean addConstructorArgRefIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName) {
		String value = element.getAttribute(attributeName);
		if (StringUtils.hasText(value)) {
			builder.addConstructorArgReference(value);
			return true;
		}
		return false;
	}

	/**
	 * Populates the bean definition constructor argument with a reference to a bean with parent id equal to the
	 * attribute if it is defined in the given element.
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be used to set the reference
	 *
	 * @return true if defined.
	 */
	public static boolean addConstructorArgParentRefIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName) {
		String value = element.getAttribute(attributeName);
		if (StringUtils.hasText(value)) {
			BeanDefinitionBuilder child = BeanDefinitionBuilder.genericBeanDefinition();
			child.setParentName(value);
			builder.addConstructorArgValue(child.getBeanDefinition());
			return true;
		}
		return false;
	}

	/**
	 * Populates the specified bean definition property with the reference to a bean. The bean reference is identified
	 * by the value from the attribute whose name is provided if that attribute is defined in the given element.
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be used as a bean reference to populate the
	 * property
	 * @param propertyName the name of the property to be populated
	 * @return true if the attribute is present and has text
	 */
	public static boolean setReferenceIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName, String propertyName) {
		String attributeValue = element.getAttribute(attributeName);
		if (StringUtils.hasText(attributeValue)) {
			builder.addPropertyReference(propertyName, attributeValue);
			return true;
		}
		return false;
	}

	/**
	 * Populates the bean definition property corresponding to the specified attributeName with the reference to a bean
	 * identified by the value of that attribute if the attribute is defined in the given element.
	 *
	 * <p>
	 * The property name will be the camel-case equivalent of the lower case hyphen separated attribute (e.g. the
	 * "foo-bar" attribute would match the "fooBar" property).
	 *
	 * @param builder the bean definition builder to be configured
	 * @param element the XML element where the attribute should be defined
	 * @param attributeName the name of the attribute whose value will be used as a bean reference to populate the
	 * property
	 * @return true if defined.
	 * @see Conventions#attributeNameToPropertyName(String)
	 */
	public static boolean setReferenceIfAttributeDefined(BeanDefinitionBuilder builder, Element element,
			String attributeName) {
		return setReferenceIfAttributeDefined(builder, element, attributeName,
				Conventions.attributeNameToPropertyName(attributeName));
	}

	/**
	 * Provides a user friendly description of an element based on its node name and, if available, its "id" attribute
	 * value. This is useful for creating error messages from within bean definition parsers.
	 *
	 * @param element The element.
	 * @return The description.
	 */
	public static String createElementDescription(Element element) {
		String elementId = "'" + element.getNodeName() + "'";
		String id = element.getAttribute("id");
		if (StringUtils.hasText(id)) {
			elementId += " with id='" + id + "'";
		}
		return elementId;
	}

	public static BeanComponentDefinition parseInnerBeanDefinition(Element element, ParserContext parserContext) {
		// parses out inner bean definition for concrete implementation if defined
		List<Element> childElements = DomUtils.getChildElementsByTagName(element, "bean");
		BeanComponentDefinition innerComponentDefinition = null;
		if (childElements != null && childElements.size() == 1) {
			Element beanElement = childElements.get(0);
			BeanDefinitionParserDelegate delegate = parserContext.getDelegate();
			BeanDefinitionHolder bdHolder = delegate.parseBeanDefinitionElement(beanElement);
			bdHolder = delegate.decorateBeanDefinitionIfRequired(beanElement, bdHolder);
			BeanDefinition inDef = bdHolder.getBeanDefinition();
			String beanName = BeanDefinitionReaderUtils.generateBeanName(inDef, parserContext.getRegistry());
			innerComponentDefinition = new BeanComponentDefinition(inDef, beanName);
			parserContext.registerBeanComponent(innerComponentDefinition);
		}

		String ref = element.getAttribute(REF_ATTRIBUTE);
		Assert.isTrue(!StringUtils.hasText(ref) || innerComponentDefinition == null, //NOSONAR
				"Ambiguous definition. Inner bean "
						+ (innerComponentDefinition == null ? innerComponentDefinition : innerComponentDefinition
								.getBeanDefinition().getBeanClassName()) + " declaration and \"ref\" " + ref
						+ " are not allowed together.");
		return innerComponentDefinition;
	}

	/**
	 * Parses 'auto-declare' and 'declared-by' attributes.
	 *
	 * @param element The element.
	 * @param builder The builder.
	 */
	public static void parseDeclarationControls(Element element, BeanDefinitionBuilder builder) {
		NamespaceUtils.setValueIfAttributeDefined(builder, element, "auto-declare", "shouldDeclare");
		String admins = element.getAttribute("declared-by");
		if (StringUtils.hasText(admins)) {
			String[] adminBeanNames = admins.split(",");
			ManagedList<BeanReference> adminBeanRefs = new ManagedList<BeanReference>();
			for (String adminBeanName : adminBeanNames) {
				adminBeanRefs.add(new RuntimeBeanReference(adminBeanName.trim()));
			}
			builder.addPropertyValue("adminsThatShouldDeclare", adminBeanRefs);
		}
		NamespaceUtils.setValueIfAttributeDefined(builder, element, "ignore-declaration-exceptions");
	}

	public static BeanDefinition createExpressionDefinitionFromValueOrExpression(String valueElementName,
			String expressionElementName, ParserContext parserContext, Element element, boolean oneRequired) {

		Assert.hasText(valueElementName, "'valueElementName' must not be empty");
		Assert.hasText(expressionElementName, "'expressionElementName' must not be empty");

		String valueElementValue = element.getAttribute(valueElementName);
		String expressionElementValue = element.getAttribute(expressionElementName);

		boolean hasAttributeValue = StringUtils.hasText(valueElementValue);
		boolean hasAttributeExpression = StringUtils.hasText(expressionElementValue);

		if (hasAttributeValue && hasAttributeExpression) {
			parserContext.getReaderContext().error("Only one of '" + valueElementName + "' or '"
					+ expressionElementName + "' is allowed", element);
		}

		if (oneRequired && (!hasAttributeValue && !hasAttributeExpression)) {
			parserContext.getReaderContext().error("One of '" + valueElementName + "' or '"
					+ expressionElementName + "' is required", element);
		}
		BeanDefinition expressionDef;
		if (hasAttributeValue) {
			expressionDef = new RootBeanDefinition(LiteralExpression.class);
			expressionDef.getConstructorArgumentValues().addGenericArgumentValue(valueElementValue);
		}
		else {
			expressionDef = createExpressionDefIfAttributeDefined(expressionElementName, element);
		}
		return expressionDef;
	}

	public static BeanDefinition createExpressionDefIfAttributeDefined(String expressionElementName, Element element) {

		Assert.hasText(expressionElementName, "'expressionElementName' must no be empty");

		String expressionElementValue = element.getAttribute(expressionElementName);

		if (StringUtils.hasText(expressionElementValue)) {
			BeanDefinitionBuilder expressionDefBuilder =
					BeanDefinitionBuilder.genericBeanDefinition(ExpressionFactoryBean.class);
			expressionDefBuilder.addConstructorArgValue(expressionElementValue);
			return expressionDefBuilder.getRawBeanDefinition();
		}
		return null;
	}

}
