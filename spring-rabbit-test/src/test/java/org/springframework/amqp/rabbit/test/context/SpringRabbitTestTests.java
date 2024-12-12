/*
 * Copyright 2020-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.test.context;

import static org.assertj.core.api.Assertions.assertThat;

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
