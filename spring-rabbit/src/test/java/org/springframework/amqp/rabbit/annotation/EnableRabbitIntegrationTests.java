/*
 * Copyright 2002-2014 the original author or authors.
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

package org.springframework.amqp.rabbit.annotation;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.rabbit.test.MessageTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

/**
 *
 * @author Stephane Nicoll
 */
@ContextConfiguration(classes = EnableRabbitIntegrationTests.EnableRabbitConfig.class)
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
public class EnableRabbitIntegrationTests {

	@ClassRule
	public static final BrokerRunning brokerRunning = BrokerRunning.isRunningWithEmptyQueues(
			"test.simple", "test.header", "test.message", "test.reply");

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Test
	public void simpleEndpoint() {
		assertEquals("FOO", rabbitTemplate.convertSendAndReceive("test.simple", "foo"));
	}

	@Test
	public void endpointWithHeader() {
		MessageProperties properties = new MessageProperties();
		properties.setHeader("prefix", "prefix-");
		Message request = MessageTestUtils.createTextMessage("foo", properties);
		Message reply = rabbitTemplate.sendAndReceive("test.header", request);
		assertEquals("prefix-FOO", MessageTestUtils.extractText(reply));
	}

	@Test
	public void endpointWithMessage() {
		MessageProperties properties = new MessageProperties();
		properties.setHeader("prefix", "prefix-");
		Message request = MessageTestUtils.createTextMessage("foo", properties);
		Message reply = rabbitTemplate.sendAndReceive("test.message", request);
		assertEquals("prefix-FOO", MessageTestUtils.extractText(reply));
	}

	@Test
	public void endpointWithComplexReply() {
		MessageProperties properties = new MessageProperties();
		properties.setHeader("foo", "fooValue");
		Message request = MessageTestUtils.createTextMessage("content", properties);
		Message reply = rabbitTemplate.sendAndReceive("test.reply", request);
		assertEquals("Wrong reply", "content", MessageTestUtils.extractText(reply));
		assertEquals("Wrong foo header", "fooValue", reply.getMessageProperties().getHeaders().get("foo"));
		assertEquals("Wrong bar header", "barValue", reply.getMessageProperties().getHeaders().get("bar"));
	}

	public static class MyService {

		@RabbitListener(queues = "test.simple")
		public String capitalize(String foo) {
			return foo.toUpperCase();
		}

		@RabbitListener(queues = "test.header")
		public String capitalizeWithHeader(@Payload String content, @Header String prefix) {
			return prefix + content.toUpperCase();
		}

		@RabbitListener(queues = "test.message")
		public String capitalizeWithMessage(org.springframework.messaging.Message<String> message) {
			return message.getHeaders().get("prefix") + message.getPayload().toUpperCase();
		}

		@RabbitListener(queues = "test.reply")
		public org.springframework.messaging.Message<?> reply(String payload, @Header String foo) {
			return MessageBuilder.withPayload(payload)
					.setHeader("foo", foo).setHeader("bar", "barValue").build();
		}
	}

	@Configuration
	@EnableRabbit
	public static class EnableRabbitConfig {

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			return factory;
		}

		@Bean
		public MyService myService() {
			return new MyService();
		}

		// Rabbit infrastructure setup

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

	}

}
