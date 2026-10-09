/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.repeatable;

import org.junit.jupiter.api.Test;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author Stephane Nicoll
 * @author Gary Russell
 *
 * @since 1.6
 *
 */
public class AnnotationDrivenNamespaceTests extends AbstractRabbitAnnotationDrivenTests {

	@Override
	@Test
	public void rabbitListenerIsRepeatable() {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"annotation-driven-no-rabbit-admin-repeatable-config.xml", getClass());
		testRabbitListenerRepeatable(context);
	}

}
