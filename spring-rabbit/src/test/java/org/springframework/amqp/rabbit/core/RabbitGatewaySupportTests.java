/*
 * Copyright 2010-present the original author or authors.
 */

package org.springframework.amqp.rabbit.core;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Mark Pollack
 * @author Chris Beams
 * @author Gary Russell
 */
public class RabbitGatewaySupportTests {

	@Test
	public void testRabbitGatewaySupportWithConnectionFactory() throws Exception {

		org.springframework.amqp.rabbit.connection.ConnectionFactory mockConnectionFactory =
				mock(org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
		final List<String> test = new ArrayList<String>();
		RabbitGatewaySupport gateway = new RabbitGatewaySupport() {
			@Override
			protected void initGateway() {
				test.add("test");
			}
		};
		gateway.setConnectionFactory(mockConnectionFactory);
		gateway.afterPropertiesSet();
		assertThat(gateway.getConnectionFactory()).as("Correct ConnectionFactory").isEqualTo(mockConnectionFactory);
		assertThat(gateway.getRabbitOperations().getConnectionFactory()).as("Correct RabbitTemplate").isEqualTo(mockConnectionFactory);
		assertThat(1).as("initGatway called").isEqualTo(test.size());
	}

	@Test
	public void testRabbitGatewaySupportWithTemplate() throws Exception {
		RabbitTemplate template = new RabbitTemplate();
		final List<String> test = new ArrayList<String>();
		RabbitGatewaySupport gateway = new RabbitGatewaySupport() {
			@Override
			protected void initGateway() {
				test.add("test");
			}
		};
		gateway.setRabbitOperations(template);
		gateway.afterPropertiesSet();
		assertThat(gateway.getRabbitOperations()).as("Correct RabbitTemplate").isEqualTo(template);
		assertThat(1).as("initGateway called").isEqualTo(test.size());
	}

}
