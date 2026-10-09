/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.amqp.rabbit.test.context;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.config.AbstractRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
@RabbitAvailable
@SpringJUnitConfig
@SpringRabbitTest
@DirtiesContext
public class SpringRabbitTestTests {

	@Autowired
	private RabbitTemplate template;

	@SuppressWarnings("unused")
	@Autowired
	private RabbitAdmin admin;

	@SuppressWarnings("unused")
	@Autowired
	private AbstractRabbitListenerContainerFactory<?> factory;

	@SuppressWarnings("unused")
	@Autowired
	private RabbitListenerEndpointRegistry registry;

	@Test
	void testAutowiring() {
		assertThat(((CachingConnectionFactory) template.getConnectionFactory()).getRabbitConnectionFactory())
			.isSameAs(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
	}

	@Configuration
	public static class Config {

	}

}
