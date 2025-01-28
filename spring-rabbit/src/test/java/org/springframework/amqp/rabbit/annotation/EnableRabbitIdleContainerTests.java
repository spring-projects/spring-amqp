/*
 * Copyright 2016-2025 the original author or authors.
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.listener.ListenerContainerIdleEvent;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 1.6
 *
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable
public class EnableRabbitIdleContainerTests {

	@Autowired
	private Listener listener;

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private Queue queue;

	@Autowired
	private RabbitListenerEndpointRegistry registry;

	@Test
	public void testIdle() throws Exception {
		assertThat(this.rabbitTemplate.convertSendAndReceive(this.queue.getName(), "foo")).isEqualTo("FOO");
		assertThat(this.rabbitTemplate.convertSendAndReceive(this.queue.getName(), "foo")).isEqualTo("FOO");
		assertThat(this.listener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.event.getListenerId()).isEqualTo("foo");
		assertThat(this.listener.event.getQueueNames()[0]).isEqualTo(this.queue.getName());
		assertThat(this.rabbitTemplate.convertSendAndReceive(this.queue.getName(), "bar")).isEqualTo("BAR");
		assertThat(this.rabbitTemplate.convertSendAndReceive(this.queue.getName(), "bar")).isEqualTo("BAR");
		assertThat(this.listener.barEventReceived).isFalse();
		MessageListenerContainer listenerContainer = registry.getListenerContainer("foo");
		assertThat(TestUtils.getPropertyValue(listenerContainer, "mismatchedQueuesFatal", Boolean.class)).isTrue();
	}

	@Configuration
	@EnableRabbit
	public static class EnableRabbitConfig {

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setIdleEventInterval(500L);
			factory.setReceiveTimeout(100L);
			factory.setMismatchedQueuesFatal(true);
			return factory;
		}

		@Bean
		public ConnectionFactory rabbitConnectionFactory() {
			CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
			connectionFactory.setHost("localhost");
			return connectionFactory;
		}

		@Bean
		public RabbitTemplate rabbitTemplate() {
			return new RabbitTemplate(rabbitConnectionFactory());
		}

		@Bean
		public RabbitAdmin rabbitAdmin() {
			return new RabbitAdmin(rabbitConnectionFactory());
		}

		@Bean
		public Queue queue() {
			return new AnonymousQueue();
		}

		@Bean
		public Listener listener() {
			return new Listener();
		}

	}

	@Component
	public static class Listener {

		private final Log logger = LogFactory.getLog(this.getClass());

		private final CountDownLatch latch = new CountDownLatch(2);

		private volatile ListenerContainerIdleEvent event;

		private boolean barEventReceived;

		@RabbitListener(id = "foo", queues = "#{queue.name}")
		public String listenFoo(String foo) {
			logger.info("foo: " + foo);
			return foo.toUpperCase();
		}

		@EventListener(condition = "event.listenerId == 'foo'")
		public void onApplicationEvent(ListenerContainerIdleEvent event) {
			if (!"foo".equals(event.getListenerId())) {
				this.barEventReceived = true;
			}
			logger.info("foo: " + event);
			this.event = event;
			this.latch.countDown();
		}

		@RabbitListener(id = "bar", queues = "#{queue.name}")
		public String listenBar(String bar) {
			logger.info("bar: " + bar);
			return bar.toUpperCase();
		}

		@EventListener(condition = "event.listenerId == 'bar'")
		public void onApplicationEventBar(ListenerContainerIdleEvent event) {
			logger.info("bar: " + event);
		}

	}

}
