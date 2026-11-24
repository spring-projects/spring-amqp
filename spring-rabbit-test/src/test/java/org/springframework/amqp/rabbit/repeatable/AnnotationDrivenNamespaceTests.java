/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
