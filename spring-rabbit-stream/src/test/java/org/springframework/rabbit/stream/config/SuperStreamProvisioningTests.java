/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.rabbit.stream.config;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 3.0
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class SuperStreamProvisioningTests extends AbstractTestContainerTests {

	@Test
	void provision(@Autowired Declarables declarables, @Autowired CachingConnectionFactory cf,
			@Autowired RabbitAdmin admin) {

		assertThat(declarables.getDeclarables()).hasSize(7);
		cf.createConnection();
		List<Queue> queues = declarables.getDeclarablesByType(Queue.class);
		assertThat(queues).extracting(que -> que.getName()).contains("test-0", "test-1", "test-2");
		queues.forEach(que -> admin.deleteQueue(que.getName()));
		declarables.getDeclarablesByType(DirectExchange.class).forEach(ex -> admin.deleteExchange(ex.getName()));
	}

	@Configuration
	public static class Config {

		@Bean
		CachingConnectionFactory cf() {
			return new CachingConnectionFactory("localhost", amqpPort());
		}

		@Bean
		RabbitAdmin admin(ConnectionFactory cf) {
			return new RabbitAdmin(cf);
		}

		@Bean
		SuperStream superStream() {
			return new SuperStream("test", 3);
		}

	}

}
