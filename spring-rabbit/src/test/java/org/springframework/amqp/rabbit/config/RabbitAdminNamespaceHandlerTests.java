/*
 * Copyright 2002-2008 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;

/**
 *
 */
public final class RabbitAdminNamespaceHandlerTests {
	
	
	@Test
	public void testParse() throws Exception {
		ApplicationContext applicationContext = new ClassPathXmlApplicationContext(
				"rabbitAdminNamespaceHandlerTests.xml", getClass());
		
		/*
		 * Map<String, PropertyPlaceholderConfigurer> beans = applicationContext
				.getBeansOfType(PropertyPlaceholderConfigurer.class);
		assertFalse("No PropertyPlaceHolderConfigurer found", beans.isEmpty());
		String s = (String) applicationContext.getBean("string");
		assertEquals("No properties replaced", "bar", s);
		 */
	}

}
