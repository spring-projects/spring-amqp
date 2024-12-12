/*
 * Copyright 2020-2024 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.converter.ContentTypeDelegatingMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.MimeType;

/**
 * @author Gary Russell
 * @since 2.5
 *
 */
@RabbitAvailable
@SpringJUnitConfig
@DirtiesContext
public class ContentTypeDelegatingMessageConverterIntegrationTests {

	@Autowired
	private Config config;

	@Autowired
	private RabbitTemplate template;

	@Autowired
	private AnonymousQueue queue1;

	@Test
	void testReplyContentType() throws InterruptedException {
		this.template.convertAndSend(this.queue1.getName(), "foo", msg -> {
			msg.getMessageProperties().setContentType("foo/bar");
			return msg;
		});
		assertThat(this.config.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.replyContentType).isEqualTo("baz/qux");
		assertThat(this.config.receivedReplyContentType).isEqualTo("baz/qux");

		this.template.convertAndSend(this.queue1.getName(), "bar", msg -> {
			msg.getMessageProperties().setContentType("foo/bar");
			return msg;
		});
		assertThat(this.config.latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.replyContentType).isEqualTo("baz/qux");
		assertThat(this.config.receivedReplyContentType).isEqualTo("foo/bar");
	}

	@Configuration
	@EnableRabbit
	public static class Config {

		final CountDownLatch latch1 = new CountDownLatch(1);

		final CountDownLatch latch2 = new CountDownLatch(2);

		volatile String replyContentType;

		volatile String receivedReplyContentType;

		@Bean
		public ConnectionFactory cf() {
			return new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		}

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(cf());
			ContentTypeDelegatingMessageConverter converter =
					new ContentTypeDelegatingMessageConverter(new SimpleMessageConverter());
			converter.addDelegate("foo/bar", new MessageConverter() {

				@Override
				public Message toMessage(Object object, MessageProperties messageProperties)
						throws MessageConversionException {

					return new Message("foo".getBytes(), messageProperties);
				}

				@Override
				public Object fromMessage(Message message) throws MessageConversionException {
					return new String(message.getBody());
				}

			});
			converter.addDelegate("baz/qux", new MessageConverter() {

				@Override
				public Message toMessage(Object object, MessageProperties messageProperties)
						throws MessageConversionException {

					Config.this.replyContentType = messageProperties.getContentType();
					messageProperties.setContentType("foo/bar");
					return new Message("foo".getBytes(), messageProperties);
				}

				@Override
				public Object fromMessage(Message message) throws MessageConversionException {
					return new String(message.getBody());
				}

			});
			factory.setMessageConverter(converter);
			return factory;
		}

		@Bean
		public RabbitTemplate template() {
			return new RabbitTemplate(cf());
		}

		@Bean
		public RabbitAdmin admin() {
			return new RabbitAdmin(cf());
		}

		@Bean
		public AnonymousQueue queue1() {
			return new AnonymousQueue();
		}

		@Bean
		public AnonymousQueue queue2() {
			return new AnonymousQueue();
		}

		@RabbitListener(queues = "#{@queue1.name}")
		@SendTo("#{@queue2.name}")
		public org.springframework.messaging.Message<String> listen1(String in) {
			MessageBuilder<String> builder = MessageBuilder.withPayload(in)
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeType.valueOf("baz/qux"));
			if ("bar".equals(in)) {
				builder.setHeader(AmqpHeaders.CONTENT_TYPE_CONVERTER_WINS, true);
			}
			return builder.build();
		}

		@RabbitListener(queues = "#{@queue2.name}")
		public void listen2(Message in) {
			this.receivedReplyContentType = in.getMessageProperties().getContentType();
			this.latch1.countDown();
			this.latch2.countDown();
		}

	}

}
