/*
 * Copyright 2019 the original author or authors.
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

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.2
 *
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable(queues = { "EnableRabbitReturnTypesTests.1", "EnableRabbitReturnTypesTests.2",
		"EnableRabbitReturnTypesTests.3", "EnableRabbitReturnTypesTests.4" })
public class EnableRabbitReturnTypesTests {

	@Test
	void testInterfaceReturn(@Autowired RabbitTemplate template) {
		Object reply = template.convertSendAndReceive("EnableRabbitReturnTypesTests.1", "3");
		assertThat(reply).isInstanceOf(Three.class);
		reply = template.convertSendAndReceive("EnableRabbitReturnTypesTests.1", "4");
		assertThat(reply).isInstanceOf(Four.class);
	}

	@Test
	void testAbstractReturn(@Autowired RabbitTemplate template) {
		Object reply = template.convertSendAndReceive("EnableRabbitReturnTypesTests.2", "3");
		assertThat(reply).isInstanceOf(Three.class);
		reply = template.convertSendAndReceive("EnableRabbitReturnTypesTests.2", "4");
		assertThat(reply).isInstanceOf(Four.class);
	}

	@Test
	void testListOfThree(@Autowired RabbitTemplate template) {
		Object reply = template.convertSendAndReceive("EnableRabbitReturnTypesTests.3", "3");
		assertThat(reply).isInstanceOf(List.class);
		assertThat(((List<?>) reply).get(0)).isInstanceOf(Three.class);
	}

	@Test
	void testGenericInterfaceReturn(@Autowired RabbitTemplate template) {
		Object reply = template.convertSendAndReceive("EnableRabbitReturnTypesTests.4", "3");
		assertThat(reply).isInstanceOf(Three.class);
		reply = template.convertSendAndReceive("EnableRabbitReturnTypesTests.4", "4");
		assertThat(reply).isInstanceOf(Four.class);
	}

	@Configuration(proxyBeanMethods = false)
	@EnableRabbit
	public static class Config<O extends One> {

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(CachingConnectionFactory cf,
				Jackson2JsonMessageConverter converter) {

			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(cf);
			factory.setMessageConverter(converter);
			factory.setDefaultRequeueRejected(false);
			return factory;
		}

		@Bean
		public RabbitTemplate template(CachingConnectionFactory cf, Jackson2JsonMessageConverter converter) {
			RabbitTemplate template = new RabbitTemplate(cf);
			template.setMessageConverter(converter);
			return template;
		}

		@Bean
		public CachingConnectionFactory cf() {
			return new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		}

		@Bean
		public Jackson2JsonMessageConverter converter() {
			return new Jackson2JsonMessageConverter();
		}

		@RabbitListener(queues = "EnableRabbitReturnTypesTests.1")
		public One listen1(String in) {
			if ("3".equals(in)) {
				return new Three();
			}
			else {
				return new Four();
			}
		}

		@RabbitListener(queues = "EnableRabbitReturnTypesTests.2")
		public Two listen2(String in) {
			if ("3".equals(in)) {
				return new Three();
			}
			else {
				return new Four();
			}
		}

		@RabbitListener(queues = "EnableRabbitReturnTypesTests.3")
		public List<Three> listen3(@SuppressWarnings("unused") String in) {
			List<Three> list = new ArrayList<>();
			list.add(new Three());
			return list;
		}

		@SuppressWarnings("unchecked")
		@RabbitListener(queues = "EnableRabbitReturnTypesTests.4")
		public O listen4(String in) {
			if ("3".equals(in)) {
				return (O) new Three();
			}
			else {
				return (O) new Four();
			}
		}

	}

	public interface One {

	}

	public static abstract class Two implements One {

		private String field;

		public String getField() {
			return this.field;
		}

		public void setField(String field) {
			this.field = field;
		}

	}

	public static class Three extends Two {

	}

	public static class Four extends Two {

	}

}
