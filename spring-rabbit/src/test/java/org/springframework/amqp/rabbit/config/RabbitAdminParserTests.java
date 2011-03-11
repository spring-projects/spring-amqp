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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.test.Log4jLevelAdjuster;
import org.springframework.beans.factory.parsing.BeanDefinitionParsingException;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StringUtils;

/**
 * 
 * @author tomas.lukosius@opencredo.com
 * 
 */
@RunWith(Parameterized.class)
public final class RabbitAdminParserTests {
	private static Log logger = LogFactory.getLog(RabbitAdminParserTests.class);

	@Rule
	public Log4jLevelAdjuster logLevels = new Log4jLevelAdjuster(Level.ERROR, RabbitAdminParserTests.class);

	// Test case expects context to be valid
	private boolean validContext = true;

	// Index of context file name used by this test (template: <class-name>-<contextIndex>-context.xml)
	private int contextIndex;

	private int expectedPhase;

	private boolean expectedAutoStartup;

	private String adminBeanName;

	private boolean initialisedWithTemplate;

	public RabbitAdminParserTests(int contextIndex, boolean validContext, String adminBeanName, int expectedPhase,
			boolean expectedAutoStartup, boolean initialisedWithTemplate) {
		super();
		this.contextIndex = contextIndex;
		this.validContext = validContext;
		this.adminBeanName = adminBeanName;
		this.expectedPhase = expectedPhase;
		this.expectedAutoStartup = expectedAutoStartup;
		this.initialisedWithTemplate = initialisedWithTemplate;
	}

	@Parameters
	public static Collection<Object[]> data() {
		Object[][] data = new Object[][] { //
		params(0, true), // #0
				params(1, false), // #1
				params(2, false), // #2
				params(3, false), // #3
				params(4, false), // #4
				params(5, true, "admin-test", 12, false, false), // #5
				params(6, true, "admin-test", 12, false, true) // #6
		};
		return Arrays.asList(data);
	}

	private static Object[] params(int index, boolean validContext) {
		return params(index, validContext, null, Integer.MIN_VALUE, true, false);
	}

	/**
	 * 
	 * @param contextIndex The index of spring context. Context file name template:
	 * &lt;class-name&gt;-&lt;contextIndex&gt;-context.xml
	 * @param validContext <code>true</code> if spring-context is expected to be loaded without failures.
	 * @param adminBeanName The bean name of expected rabbit-admin. If its not specified - rabbit-admin will be
	 * retrieved by type.
	 * @param expectedPhase 'phase' expected in {@link RabbitAdmin}.
	 * @param expectedAutoStartup 'autoStartup' expected in {@link RabbitAdmin}.
	 * @param initialisedWithTemplat <code>true</code> if {@link RabbitAdmin} in spring-context initialized by passing
	 * {@link RabbitTemplate} as constructor parameter, <code>false</code> - initialized by passing
	 * {@link ConnectionFactory} as constructor parameter.
	 * @return
	 */
	private static Object[] params(int contextIndex, boolean validContext, String adminBeanName, int expectedPhase,
			boolean expectedAutoStartup, boolean initialisedWithTemplat) {
		return new Object[] { contextIndex, validContext, adminBeanName, expectedPhase, expectedAutoStartup,
				initialisedWithTemplat };
	}

	@Test
	public void testParse() throws Exception {
		// Create context
		XmlBeanFactory beanFactory = loadContext();
		if (beanFactory == null) {
			// Context was invalid
			return;
		}

		// Validate values
		RabbitAdmin admin;
		if (StringUtils.hasText(adminBeanName)) {
			admin = beanFactory.getBean(adminBeanName, RabbitAdmin.class);
		} else {
			admin = beanFactory.getBean(RabbitAdmin.class);
		}
		assertEquals(expectedPhase, admin.getPhase());
		assertEquals(expectedAutoStartup, admin.isAutoStartup());
		assertEquals(beanFactory.getBean(ConnectionFactory.class), admin.getRabbitTemplate().getConnectionFactory());

		if (initialisedWithTemplate) {
			assertEquals(beanFactory.getBean(RabbitTemplate.class), admin.getRabbitTemplate());
		}

	}

	/**
	 * Load application context. Fail if tests expects invalid spring-context, but spring-context is valid.
	 * @return
	 */
	private XmlBeanFactory loadContext() {
		XmlBeanFactory beanFactory = null;
		try {
			// Resource file name template: <class-name>-<contextIndex>-context.xml
			ClassPathResource resource = new ClassPathResource(getClass().getSimpleName() + "-" + contextIndex
					+ "-context.xml", getClass());
			beanFactory = new XmlBeanFactory(resource);
			if (!validContext) {
				fail("Context " + resource + " suppose to fail");
			}
		} catch (BeanDefinitionParsingException e) {
			if (validContext) {
				// Context expected to be valid - throw an exception up
				throw e;
			}

			logger.warn("Failure was expected", e);
		}
		return beanFactory;
	}
}
