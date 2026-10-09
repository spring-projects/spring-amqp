/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.factory.parsing.BeanDefinitionParsingException;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 *
 * @author tomas.lukosius@opencredo.com
 * @author Gary Russell
 * @author Artem Bilan
 *
 */
public final class AdminParserTests {

	private static final Log logger = LogFactory.getLog(AdminParserTests.class);

	// Specifies if a test case expects context to be valid or not: true - context expects to be valid.
	private boolean validContext = true;

	// Index of a context file used by this test case. Context file name has such a template:
	// <class-name>-<contextIndex>-context.xml.
	private int contextIndex;

	private boolean expectedAutoStartup;

	@Test
	public void testValid0() {
		this.expectedAutoStartup = true;
		this.contextIndex = 0;
		this.validContext = true;
		doTest(false);
	}

	@Test
	public void testInvalid1() {
		this.contextIndex = 1;
		this.validContext = false;
		doTest(false);
	}

	@Test
	public void testValid2() {
		this.contextIndex = 2;
		this.validContext = true;
		doTest(true);
	}

	private void doTest(boolean explicit) {
		// Create context
		DefaultListableBeanFactory beanFactory = loadContext();
		if (beanFactory == null) {
			// Context was invalid
			return;
		}

		// Validate values
		RabbitAdmin admin = beanFactory.getBean(RabbitAdmin.class);
		assertThat(admin.isAutoStartup()).isEqualTo(this.expectedAutoStartup);
		assertThat(admin.getRabbitTemplate().getConnectionFactory())
				.isEqualTo(beanFactory.getBean(ConnectionFactory.class));

		assertThat(TestUtils.<Boolean>getPropertyValue(admin, "explicitDeclarationsOnly")).isEqualTo(explicit);
	}

	private DefaultListableBeanFactory loadContext() {
		DefaultListableBeanFactory beanFactory;
		try {
			// Resource file name template: <class-name>-<contextIndex>-context.xml
			ClassPathResource resource = new ClassPathResource(getClass().getSimpleName() + "-" + contextIndex
					+ "-context.xml", getClass());
			beanFactory = new DefaultListableBeanFactory();
			XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beanFactory);
			reader.loadBeanDefinitions(resource);
			if (!this.validContext) {
				fail("Context " + resource + " failed to load");
			}
		}
		catch (BeanDefinitionParsingException e) {
			if (this.validContext) {
				// Context expected to be valid - throw an exception up
				throw e;
			}

			logger.warn("Failure was expected", e);
			beanFactory = null;
		}
		return beanFactory;
	}

}
