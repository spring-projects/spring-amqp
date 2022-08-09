/*
 * Copyright 2022 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessagePropertiesBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Gary Russell
 * @since 2.8
 *
 */
@SpringJUnitConfig
@RabbitAvailable(queues = { "op.1", "op.2" })
public class OptionalPayloadTests {

	@Test
	void optionals(@Autowired RabbitTemplate template, @Autowired Listener listener)
			throws JsonProcessingException, AmqpException, InterruptedException {

		ObjectMapper objectMapper = new ObjectMapper();
		template.send("op.1", MessageBuilder.withBody(objectMapper.writeValueAsBytes("foo"))
				.andProperties(MessagePropertiesBuilder.newInstance()
						.setContentType("application/json")
						.build())
				.build());
		template.send("op.1", MessageBuilder.withBody(objectMapper.writeValueAsBytes(null))
				.andProperties(MessagePropertiesBuilder.newInstance()
						.setContentType("application/json")
						.build())
				.build());
		template.send("op.2", MessageBuilder.withBody(objectMapper.writeValueAsBytes("bar"))
				.andProperties(MessagePropertiesBuilder.newInstance()
						.setContentType("application/json")
						.build())
				.build());
		template.send("op.2", MessageBuilder.withBody(objectMapper.writeValueAsBytes(null))
				.andProperties(MessagePropertiesBuilder.newInstance()
						.setContentType("application/json")
						.build())
				.build());
		assertThat(listener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.deOptionaled).containsExactlyInAnyOrder("foo", null, "bar", "baz");
	}

	@Configuration
	@EnableRabbit
	public static class Config {

		@Bean
		RabbitTemplate template() {
			return new RabbitTemplate(rabbitConnectionFactory());
		}

		@Bean
		SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setMessageConverter(converter());
			return factory;
		}

		@Bean
		ConnectionFactory rabbitConnectionFactory() {
			return new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		}

		@Bean
		Jackson2JsonMessageConverter converter() {
			Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
			converter.setNullAsOptionalEmpty(true);
			return converter;
		}

		@Bean
		Listener listener() {
			return new Listener();
		}

	}

	static class Listener {

		final CountDownLatch latch = new CountDownLatch(4);

		List<String> deOptionaled = new ArrayList<>();

		@RabbitListener(queues = "op.1")
		void listen(@Payload(required = false) String payload) {
			this.deOptionaled.add(payload);
			this.latch.countDown();
		}

		@RabbitListener(queues = "op.2")
		void listen(Optional<String> optional) {
			this.deOptionaled.add(optional.orElse("baz"));
			this.latch.countDown();
		}

	}

}
