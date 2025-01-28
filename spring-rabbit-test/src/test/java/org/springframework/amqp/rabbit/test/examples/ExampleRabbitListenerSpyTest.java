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

package org.springframework.amqp.rabbit.test.examples;

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
import org.springframework.amqp.rabbit.test.RabbitListenerTest;
import org.springframework.amqp.rabbit.test.RabbitListenerTestHarness;
import org.springframework.amqp.rabbit.test.mockito.LatchCountDownAndCallRealMethodAnswer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.6
 *
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable
public class ExampleRabbitListenerSpyTest {

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private Queue queue1;

	@Autowired
	private Queue queue2;

	@Autowired
	private RabbitListenerTestHarness harness;

	@Test
	public void testTwoWay() {
		assertThat(this.rabbitTemplate.convertSendAndReceive(this.queue1.getName(), "foo")).isEqualTo("FOO");

		Listener listener = this.harness.getSpy("foo");
		assertThat(listener).isNotNull();
		verify(listener).foo("foo");
	}

	@Test
	public void testOneWay() throws Exception {
		Listener listener = this.harness.getSpy("bar");
		assertThat(listener).isNotNull();

		LatchCountDownAndCallRealMethodAnswer answer = this.harness.getLatchAnswerFor("bar", 2);
		willAnswer(answer).given(listener).foo(anyString(), anyString());

		this.rabbitTemplate.convertAndSend(this.queue2.getName(), "bar");
		this.rabbitTemplate.convertAndSend(this.queue2.getName(), "baz");

		assertThat(answer.await(10)).isTrue();
		verify(listener).foo("bar", this.queue2.getName());
		verify(listener).foo("baz", this.queue2.getName());
	}

	@Configuration
	@EnableRabbit
	@RabbitListenerTest
	public static class Config {

		@Bean
		public Listener listener() {
			return new Listener();
		}

		@Bean
		public ConnectionFactory connectionFactory() {
			return new CachingConnectionFactory("localhost");
		}

		@Bean
		public Queue queue1() {
			return new AnonymousQueue();
		}

		@Bean
		public Queue queue2() {
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

	}

	public static class Listener {

		@RabbitListener(id = "foo", queues = "#{queue1.name}")
		public String foo(String foo) {
			return foo.toUpperCase();
		}

		@RabbitListener(id = "bar", queues = "#{queue2.name}")
		public void foo(@Payload String foo, @Header("amqp_receivedRoutingKey") String rk) {
		}

	}

}
