/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.1
 *
 */
@SpringJUnitConfig
@RabbitAvailable
public class BrokerEventListenerTests {

	@Autowired
	public Config config;

	@Test
	public void testEvents() throws Exception {
		this.config.connectionFactory().createConnection().close();
		if (this.config.eventListener().isBindingsFailed()) {
			//missing plugin
			return;
		}
		RabbitAdmin admin = new RabbitAdmin(this.config.connectionFactory());
		Queue queue = new AnonymousQueue();
		admin.declareQueue(queue);
		admin.deleteQueue(queue.getName());
		assertThat(this.config.latch.await(10, TimeUnit.SECONDS)).isTrue();
		Map<String, Object> headers = this.config.events.get("channel.created");
		assertThat(headers).isNotNull();
		headers = this.config.events.get("queue.created");
		assertThat(headers).isNotNull();
		assertThat(headers.get("name")).isEqualTo(queue.getName());
		headers = this.config.events.get("queue.deleted");
		assertThat(headers).isNotNull();
		assertThat(headers.get("name")).isEqualTo(queue.getName());
	}

	@Configuration
	public static class Config {

		private final CountDownLatch latch = new CountDownLatch(3);

		private final Map<String, Map<String, Object>> events = new HashMap<>();

		@Bean
		public BrokerEventListener eventListener() {
			return new BrokerEventListener(connectionFactory(), "user.#", "channel.#", "queue.#");
		}

		@Bean
		public ConnectionFactory connectionFactory() {
			return new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		}

		@org.springframework.context.event.EventListener
		public void listener(BrokerEvent event) {
			this.events.put(event.getEventType(), event.getHeaders());
			this.latch.countDown();
		}

	}

}
