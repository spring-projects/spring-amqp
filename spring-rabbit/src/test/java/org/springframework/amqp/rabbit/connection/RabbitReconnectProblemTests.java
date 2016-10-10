/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.concurrent.Semaphore;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Lars Hvile
 * @author Gary Russell
 * @since 1.5.6
 *
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@Ignore("Requires user interaction")
public class RabbitReconnectProblemTests {

	@Autowired
	CachingConnectionFactory connFactory;

	@Autowired
	AmqpAdmin admin;

	@Autowired
	AmqpTemplate template;

	final Queue myQueue = new Queue("my-queue");

	@Before
	public void setup() {
		admin.declareQueue(myQueue);
	}

	@Test
	public void surviveAReconnect() throws Exception {
		checkIt(0);
		System .out .println("Restart RabbitMQ & press any key...");
		System.in.read();

		for (int i = 1; i < 10; i++) {
			checkIt(i);
		}

		int availablePermits = ((Semaphore) TestUtils.getPropertyValue(this.connFactory, "checkoutPermits", Map.class).values()
				.iterator().next()).availablePermits();
		System .out .println("Permits after test: " + availablePermits);
		assertEquals(2, availablePermits);
	}

	void checkIt(int counter) {
		System .out .println("\n#" + counter);
		template.receive(myQueue.getName());
		System .out .println("OK");
	}

	@Configuration
	static class Cfg {

		@Bean
		CachingConnectionFactory connectionFactory() throws Exception {
			final ConnectionFactory cf = new ConnectionFactory();
			cf.setUri("amqp://localhost");

			final CachingConnectionFactory result = new CachingConnectionFactory(cf);
			result.setChannelCacheSize(2);
			result.setChannelCheckoutTimeout(2000);

			return result;
		}

		@Bean
		AmqpAdmin rabbitAdmin() throws Exception {
			return new RabbitAdmin(connectionFactory());
		}

		@Bean
		AmqpTemplate rabbitTemplate() throws Exception {
			return new RabbitTemplate(connectionFactory());
		}
	}
}
