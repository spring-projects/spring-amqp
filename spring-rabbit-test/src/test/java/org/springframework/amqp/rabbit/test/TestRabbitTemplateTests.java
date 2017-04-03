/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.amqp.rabbit.test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringRunner;

import com.rabbitmq.client.Channel;


/**
 * @author Gary Russell
 * @since 2.0
 *
 */
@RunWith(SpringRunner.class)
public class TestRabbitTemplateTests {

	@Autowired
	private TestRabbitTemplate template;

	@Autowired
	private Config config;

	@Test
	public void testSimpleSends() {
		this.template.convertAndSend("foo", "hello1");
		assertThat(this.config.fooIn, equalTo("foo:hello1"));
		this.template.convertAndSend("bar", "hello2");
		assertThat(this.config.barIn, equalTo("bar:hello2"));
		assertThat(this.config.smlc1In, equalTo("smlc1:"));
		this.template.convertAndSend("foo", "hello3");
		assertThat(this.config.fooIn, equalTo("foo:hello1"));
		this.template.convertAndSend("bar", "hello4");
		assertThat(this.config.barIn, equalTo("bar:hello2"));
		assertThat(this.config.smlc1In, equalTo("smlc1:hello3hello4"));
	}

	@Test
	public void testSendAndReceive() {
		assertThat(this.template.convertSendAndReceive("baz", "hello"), equalTo("baz:hello"));
	}

	@Configuration
	@EnableRabbit
	public static class Config {

		public String fooIn = "";

		public String barIn = "";

		public String smlc1In = "smlc1:";

		@Bean
		public TestRabbitTemplate template() throws IOException {
			return new TestRabbitTemplate(connectionFactory());
		}

		@Bean
		public ConnectionFactory connectionFactory() throws IOException {
			ConnectionFactory factory = mock(ConnectionFactory.class);
			Connection connection = mock(Connection.class);
			Channel channel = mock(Channel.class);
			willReturn(connection).given(factory).createConnection();
			willReturn(channel).given(connection).createChannel(anyBoolean());
			given(channel.isOpen()).willReturn(true);
			return factory;
		}

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() throws IOException {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(connectionFactory());
			return factory;
		}

		@RabbitListener(queues = "foo")
		public void foo(String in) {
			this.fooIn += "foo:" + in;
		}

		@RabbitListener(queues = "bar")
		public void bar(String in) {
			this.barIn += "bar:" + in;
		}

		@RabbitListener(queues = "baz")
		public String baz(String in) {
			return "baz:" + in;
		}

		@Bean
		public SimpleMessageListenerContainer smlc1() throws IOException {
			SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory());
			container.setQueueNames("foo", "bar");
			container.setMessageListener(new MessageListenerAdapter(new Object() {

				@SuppressWarnings("unused")
				public void handleMessage(String in) {
					smlc1In += in;
				}

			}));
			return container;
		}

	}

}
