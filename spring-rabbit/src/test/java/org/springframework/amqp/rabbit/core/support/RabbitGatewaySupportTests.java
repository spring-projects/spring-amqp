package org.springframework.amqp.rabbit.core.support;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;


public class RabbitGatewaySupportTests {

	@Test
	public void testRabbitGatewaySupportWithConnectionFactory() throws Exception {
		
		org.springframework.amqp.rabbit.connection.ConnectionFactory mockConnectionFactory = mock(org.springframework.amqp.rabbit.connection.ConnectionFactory.class);
		final List test = new ArrayList();
		RabbitGatewaySupport gateway = new RabbitGatewaySupport() {
			protected void initGateway() {
				test.add("test");
			}
		};
		gateway.setConnectionFactory(mockConnectionFactory);
		gateway.afterPropertiesSet();
		assertEquals("Correct ConnectionFactory", mockConnectionFactory, gateway.getConnectionFactory());
		assertEquals("Correct RabbitTemplate", mockConnectionFactory, gateway.getRabbitTemplate().getConnectionFactory());
		assertEquals("initGatway called", test.size(), 1);
	}
	
	@Test
	public void testRabbitGatewaySupportWithJmsTemplate() throws Exception {
		RabbitTemplate template = new RabbitTemplate();
		final List test = new ArrayList();
		RabbitGatewaySupport gateway = new RabbitGatewaySupport() {
			protected void initGateway() {
				test.add("test");
			}
		};
		gateway.setRabbitTemplate(template);
		gateway.afterPropertiesSet();
		assertEquals("Correct RabbitTemplate", template, gateway.getRabbitTemplate());
		assertEquals("initGateway called", test.size(), 1);		
	}
}
