/*
 * Copyright 2002-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.parsing.BeanDefinitionParsingException;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StringUtils;

/**
 *
 * @author tomas.lukosius@opencredo.com
 * @author Gary Russell
 *
 */
public final class AdminParserTests {

	private static Log logger = LogFactory.getLog(AdminParserTests.class);

	// Specifies if test case expects context to be valid or not: true - context expects to be valid.
	private boolean validContext = true;

	// Index of context file used by this test case. Context file name has such template:
	// <class-name>-<contextIndex>-context.xml.
	private int contextIndex;

	private boolean expectedAutoStartup;

	private String adminBeanName;

	private boolean initialisedWithTemplate;

	@Test
	public void testInvalid() throws Exception {
		contextIndex = 1;
		validContext = false;
		doTest();
	}

	@Test
	public void testValid() throws Exception {
		contextIndex = 2;
		validContext = true;
		doTest();
	}

	private void doTest() throws Exception {
		// Create context
		DefaultListableBeanFactory beanFactory = loadContext();
		if (beanFactory == null) {
			// Context was invalid
			return;
		}

		// Validate values
		RabbitAdmin admin;
		if (StringUtils.hasText(adminBeanName)) {
			admin = beanFactory.getBean(adminBeanName, RabbitAdmin.class);
		}
		else {
			admin = beanFactory.getBean(RabbitAdmin.class);
		}
		assertThat(admin.isAutoStartup()).isEqualTo(expectedAutoStartup);
		assertThat(admin.getRabbitTemplate().getConnectionFactory()).isEqualTo(beanFactory.getBean(ConnectionFactory.class));

		if (initialisedWithTemplate) {
			assertThat(admin.getRabbitTemplate()).isEqualTo(beanFactory.getBean(RabbitTemplate.class));
		}

	}

	/**
	 * Load application context. Fail if tests expects invalid spring-context, but spring-context is valid.
	 * @return
	 */
	private DefaultListableBeanFactory loadContext() {
		DefaultListableBeanFactory beanFactory = null;
		try {
			// Resource file name template: <class-name>-<contextIndex>-context.xml
			ClassPathResource resource = new ClassPathResource(getClass().getSimpleName() + "-" + contextIndex
					+ "-context.xml", getClass());
			beanFactory = new DefaultListableBeanFactory();
			XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beanFactory);
			reader.loadBeanDefinitions(resource);
			if (!validContext) {
				fail("Context " + resource + " failed to load");
			}
		}
		catch (BeanDefinitionParsingException e) {
			if (validContext) {
				// Context expected to be valid - throw an exception up
				throw e;
			}

			logger.warn("Failure was expected", e);
			beanFactory = null;
		}
		return beanFactory;
	}
}
