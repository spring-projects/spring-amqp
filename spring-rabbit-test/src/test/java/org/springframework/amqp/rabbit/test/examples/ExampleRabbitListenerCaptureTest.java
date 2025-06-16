/*
 * Copyright 2016-present the original author or authors.
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

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.test.RabbitListenerTest;
import org.springframework.amqp.rabbit.test.RabbitListenerTestHarness;
import org.springframework.amqp.rabbit.test.RabbitListenerTestHarness.InvocationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.6
 *
 */
@ContextConfiguration(loader = ExampleRabbitListenerCaptureTest.NoBeansOverrideAnnotationConfigContextLoader.class)
@ExtendWith(SpringExtension.class)
@DirtiesContext
@RabbitAvailable
public class ExampleRabbitListenerCaptureTest {

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private Queue queue1;

	@Autowired
	private Queue queue2;

	@Autowired
	private RabbitListenerTestHarness harness;

	@Test
	public void testTwoWay() throws Exception {
		assertThat(this.rabbitTemplate.convertSendAndReceive(this.queue1.getName(), "foo")).isEqualTo("FOO");

		InvocationData invocationData = this.harness.getNextInvocationDataFor("foo", 10, TimeUnit.SECONDS);
		assertThat(invocationData).isNotNull();
		assertThat((String) invocationData.getArguments()[0]).isEqualTo("foo");
		assertThat((String) invocationData.getResult()).isEqualTo("FOO");
	}

	@Test
	public void testOneWay() throws Exception {
		this.rabbitTemplate.convertAndSend(this.queue2.getName(), "bar");
		this.rabbitTemplate.convertAndSend(this.queue2.getName(), "baz");
		this.rabbitTemplate.convertAndSend(this.queue2.getName(), "ex");

		InvocationData invocationData = this.harness.getNextInvocationDataFor("bar", 10, TimeUnit.SECONDS);
		assertThat(invocationData).isNotNull();
		Object[] args = invocationData.getArguments();
		assertThat((String) args[0]).isEqualTo("bar");
		assertThat((String) args[1]).isEqualTo(queue2.getName());

		invocationData = this.harness.getNextInvocationDataFor("bar", 10, TimeUnit.SECONDS);
		assertThat(invocationData).isNotNull();
		args = invocationData.getArguments();
		assertThat((String) args[0]).isEqualTo("baz");
		assertThat((String) args[1]).isEqualTo(queue2.getName());

		invocationData = this.harness.getNextInvocationDataFor("bar", 10, TimeUnit.SECONDS);
		assertThat(invocationData).isNotNull();
		args = invocationData.getArguments();
		assertThat((String) args[0]).isEqualTo("ex");
		assertThat((String) args[1]).isEqualTo(queue2.getName());
		assertThat(invocationData.getThrowable()).isNotNull();
		assertThat(invocationData.getThrowable().getMessage()).isEqualTo("ex");

		invocationData = this.harness.getNextInvocationDataFor("bar", 10, TimeUnit.SECONDS);
		assertThat(invocationData).isNotNull();
		args = invocationData.getArguments();
		assertThat((String) args[0]).isEqualTo("ex");
		assertThat((String) args[1]).isEqualTo(queue2.getName());
		assertThat(invocationData.getThrowable()).isNull();
	}

	@Configuration
	@RabbitListenerTest(spy = false, capture = true)
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
		public Queue queue3() {
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

		private boolean failed;

		@RabbitListener(id = "foo", queues = "#{queue1.name}")
		public String foo(String foo) {
			return foo.toUpperCase();
		}

		@RabbitListener(id = "bar", queues = "#{queue2.name}")
		@RabbitListener(id = "bar2", queues = "#{queue3.name}")
		public void foo(@Payload String foo, @Header("amqp_receivedRoutingKey") String rk) {
			if (!failed && foo.equals("ex")) {
				failed = true;
				throw new RuntimeException(foo);
			}
			failed = false;
		}

	}


	public static class NoBeansOverrideAnnotationConfigContextLoader extends AnnotationConfigContextLoader {

		@Override
		protected void customizeBeanFactory(DefaultListableBeanFactory beanFactory) {
			beanFactory.setAllowBeanDefinitionOverriding(false);
		}

	}

}

