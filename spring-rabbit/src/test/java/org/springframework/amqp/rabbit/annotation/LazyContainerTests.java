/*
 * Copyright 2019-2025 the original author or authors.
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

package org.springframework.amqp.rabbit.annotation;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 2.1.5
 *
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable(queues = "test.lazy")
public class LazyContainerTests {

	@Autowired
	private ConfigurableApplicationContext context;

	@Autowired
	private ObjectProvider<LazyListener> lazyListenerProvider;

	@Autowired
	private  RabbitTemplate rabbitTemplate;

	@Test
	void lazy() {
		this.context.getBeanFactory().registerSingleton("clearTheByTypeCache", "foo");
		long t1 = System.currentTimeMillis();
		this.lazyListenerProvider.getIfAvailable();
		assertThat(System.currentTimeMillis() - t1).isLessThan(30_000L);
		Object reply = this.rabbitTemplate.convertSendAndReceive("test.lazy", "lazy");
		assertThat(reply).isNotNull();
		assertThat(reply).isEqualTo("LAZY");
	}

	@Configuration
	@EnableRabbit
	public static class Config {
		@Bean
		public CachingConnectionFactory cf() {
			return new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning()
					.getConnectionFactory());
		}

		@Bean
		public RabbitTemplate template() {
			return new RabbitTemplate(cf());
		}

		@Bean
		public RabbitAdmin admin() {
			return new RabbitAdmin(template());
		}

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory cf = new SimpleRabbitListenerContainerFactory();
			cf.setConnectionFactory(cf());
			return cf;
		}

		@Bean
		public Queue queue() {
			return new Queue("test.lazy");
		}

		@Bean
		@Lazy
		public LazyListener listener() {
			return new LazyListener();
		}

		@RabbitListener(queues = "test.lazy")
		public String listen(String in) {
			return in.toUpperCase();
		}

	}

	public static class LazyListener {

		@RabbitListener(queues = "test.lazy", concurrency = "2")
		public String listenLazily(String in) {
			return in.toUpperCase();
		}

	}

}
