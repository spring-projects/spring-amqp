/*
 * Copyright 2010-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

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
		assertEquals("Correct ConnectionFactory", mockConnectionFactory, gateway.getConnectionFactory());
		assertEquals("Correct RabbitTemplate", mockConnectionFactory,
				gateway.getRabbitOperations().getConnectionFactory());
		assertEquals("initGatway called", test.size(), 1);
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
		assertEquals("Correct RabbitTemplate", template, gateway.getRabbitOperations());
		assertEquals("initGateway called", test.size(), 1);
	}

}
