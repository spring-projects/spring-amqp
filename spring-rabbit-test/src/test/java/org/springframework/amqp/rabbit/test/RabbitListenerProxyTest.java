/*
 * Copyright 2020-2025 the original author or authors.
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

package org.springframework.amqp.rabbit.test;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.test.mockito.LatchCountDownAndCallRealMethodAnswer;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.verify;

/**
 * @author Miguel Gross Valle
 *
 * @since 2.1.15
 *
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable
public class RabbitListenerProxyTest {

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private Queue queue;

	@Autowired
	private RabbitListenerTestHarness harness;

	@Test
	public void testProxiedListenerSpy() throws Exception {
		Listener listener = this.harness.getSpy("foo");
		assertThat(listener).isNotNull();

		LatchCountDownAndCallRealMethodAnswer answer = this.harness.getLatchAnswerFor("foo", 1);
		willAnswer(answer).given(listener).foo(anyString());

		this.rabbitTemplate.convertAndSend(this.queue.getName(), "foo");

		assertThat(answer.await(10)).isTrue();
		verify(listener).foo("foo");
		assertThat(answer.getExceptions()).isEmpty();
	}

	@Configuration
	@EnableRabbit
	@RabbitListenerTest
	public static class Config {

		@Bean
		public Listener listener() {
			return (Listener) new ProxyFactory(new Listener(dependency())).getProxy();
		}

		@Bean
		public ConnectionFactory connectionFactory() {
			return new CachingConnectionFactory("localhost");
		}

		@Bean
		public Queue queue() {
			return new AnonymousQueue();
		}

		@Bean
		public RabbitAdmin admin(ConnectionFactory cf) {
			return new RabbitAdmin(cf);
		}

		@Bean
		public RabbitTemplate template(ConnectionFactory cf) {
			return new RabbitTemplate(cf);
		}

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory cf) {
			SimpleRabbitListenerContainerFactory containerFactory = new SimpleRabbitListenerContainerFactory();
			containerFactory.setConnectionFactory(cf);
			return containerFactory;
		}

		@Bean
		public String dependency() {
			return "dependency";
		}

	}

	public static class Listener {

		private final String dependency;

		public Listener(String dependency) {
			this.dependency = dependency;
		}

		@RabbitListener(id = "foo", queues = "#{queue.name}")
		public void foo(String foo) {
			this.dependency.substring(0);
		}
	}

}
