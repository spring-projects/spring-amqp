/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.amqp.rabbit.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;

/**
 * @author Dave Syer
 * @author Gary Russell
 *
 */
public class QueueParserPlaceholderTests extends QueueParserTests {

	@BeforeEach
	@Override
	public void setUpDefaultBeanFactory() {
		beanFactory = new GenericXmlApplicationContext(
				new ClassPathResource(getClass().getSimpleName() + "-context.xml", getClass()));
	}

	@AfterEach
	public void closeBeanFactory() {
		if (beanFactory != null) {
			((ConfigurableApplicationContext) beanFactory).close();
		}
	}

}
