/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.context.support.GenericApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 2.4
 *
 */
@RabbitAvailable
public class ContainerAdminTests {

	@Test
	void findAdminInParentContext() {
		GenericApplicationContext parent = new GenericApplicationContext();
		CachingConnectionFactory cf =
				new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		RabbitAdmin admin = new RabbitAdmin(cf);
		parent.registerBean(RabbitAdmin.class, () -> admin);
		parent.refresh();
		GenericApplicationContext child = new GenericApplicationContext(parent);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(cf);
		container.setReceiveTimeout(10);
		child.registerBean(SimpleMessageListenerContainer.class, () -> container);
		child.refresh();
		container.start();
		assertThat(TestUtils.getPropertyValue(container, "amqpAdmin")).isSameAs(admin);
		container.stop();
	}

}
