/*
 * Copyright 2002-2019 the original author or authors.
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

import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.AbstractExchange;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.rabbit.config.MessageListenerTestContainer;
import org.springframework.amqp.rabbit.config.RabbitListenerContainerTestFactory;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.stereotype.Component;

/**
 * @author Wander Costa
 */
class MultiRabbitIntegrationTests {

	@Test
	void multipleSimpleMessageListeners() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(MultiConfig.class,
				SimpleMessageListenerTestBean.class);

		Map<String, RabbitListenerContainerTestFactory> factories = context
				.getBeansOfType(RabbitListenerContainerTestFactory.class, false, false);
		Assertions.assertThat(factories).hasSize(3);

		factories.values().forEach(factory -> {
			Assertions.assertThat(factory.getListenerContainers().size())
					.as("One container should have been registered").isEqualTo(1);
			MessageListenerTestContainer container = factory.getListenerContainers().get(0);

			RabbitListenerEndpoint endpoint = container.getEndpoint();
			Assertions.assertThat(endpoint.getClass()).as("Wrong endpoint type")
					.isEqualTo(MethodRabbitListenerEndpoint.class);
			MethodRabbitListenerEndpoint methodEndpoint = (MethodRabbitListenerEndpoint) endpoint;
			Assertions.assertThat(methodEndpoint.getBean()).isNotNull();
			Assertions.assertThat(methodEndpoint.getMethod()).isNotNull();

			SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer();
			methodEndpoint.setupListenerContainer(listenerContainer);
			Assertions.assertThat(listenerContainer.getMessageListener()).isNotNull();
		});

		context.close(); // Close and stop the listeners
	}

	@Test
	void testDeclarablesMatchProperRabbitAdmin() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(MultiConfig.class,
				AutoBindingListenerTestBeans.class);

		Map<String, RabbitListenerContainerTestFactory> factories = context
				.getBeansOfType(RabbitListenerContainerTestFactory.class, false, false);
		Assertions.assertThat(factories).hasSize(3);

		BiFunction<RabbitAdmin, Declarable, Boolean> declares = (admin, dec) -> dec.getDeclaringAdmins().size() == 1
				&& dec.getDeclaringAdmins().contains(admin);

		Map<String, AbstractExchange> exchanges = context.getBeansOfType(AbstractExchange.class, false, false)
				.values().stream().collect(Collectors.toMap(AbstractExchange::getName, v -> v));
		Assertions.assertThat(exchanges).hasSize(3);
		Assertions.assertThat(declares.apply(MultiConfig.DEFAULT_RABBIT_ADMIN, exchanges.get("testExchange"))).isTrue();
		Assertions.assertThat(declares.apply(MultiConfig.RABBIT_ADMIN_BROKER_B, exchanges.get("testExchangeB")))
				.isTrue();
		Assertions.assertThat(declares.apply(MultiConfig.RABBIT_ADMIN_BROKER_C, exchanges.get("testExchangeC")))
				.isTrue();

		Map<String, org.springframework.amqp.core.Queue> queues = context
				.getBeansOfType(org.springframework.amqp.core.Queue.class, false, false)
				.values().stream().collect(Collectors.toMap(org.springframework.amqp.core.Queue::getName, v -> v));
		Assertions.assertThat(queues).hasSize(3);
		Assertions.assertThat(declares.apply(MultiConfig.DEFAULT_RABBIT_ADMIN, queues.get("testQueue"))).isTrue();
		Assertions.assertThat(declares.apply(MultiConfig.RABBIT_ADMIN_BROKER_B, queues.get("testQueueB"))).isTrue();
		Assertions.assertThat(declares.apply(MultiConfig.RABBIT_ADMIN_BROKER_C, queues.get("testQueueC"))).isTrue();

		Map<String, Binding> bindings = context.getBeansOfType(Binding.class, false, false)
				.values().stream().collect(Collectors.toMap(Binding::getRoutingKey, v -> v));
		Assertions.assertThat(bindings).hasSize(3);
		Assertions.assertThat(declares.apply(MultiConfig.DEFAULT_RABBIT_ADMIN, bindings.get("testKey"))).isTrue();
		Assertions.assertThat(declares.apply(MultiConfig.RABBIT_ADMIN_BROKER_B, bindings.get("testKeyB"))).isTrue();
		Assertions.assertThat(declares.apply(MultiConfig.RABBIT_ADMIN_BROKER_C, bindings.get("testKeyC"))).isTrue();

		context.close(); // Close and stop the listeners
	}

	@Component
	static class AutoBindingListenerTestBeans {

		@RabbitListener(bindings = @QueueBinding(
				exchange = @Exchange("testExchange"),
				value = @Queue("testQueue"),
				key = "testKey"))
		public void handleIt(String body) {
		}

		@RabbitListener(containerFactory = "brokerB", bindings = @QueueBinding(
				exchange = @Exchange("testExchangeB"),
				value = @Queue("testQueueB"),
				key = "testKeyB"))
		public void handleItB(String body) {
		}

		@RabbitListener(containerFactory = "brokerC", bindings = @QueueBinding(
				exchange = @Exchange("testExchangeC"),
				value = @Queue("testQueueC"),
				key = "testKeyC"))
		public void handleItC(String body) {
		}
	}

	@Component
	static class SimpleMessageListenerTestBean {

		@RabbitListener(queues = "testQueue")
		public void handleIt(String body) {
		}

		@RabbitListener(queues = "testQueueB", containerFactory = "brokerB")
		public void handleItB(String body) {
		}

		@RabbitListener(queues = "testQueueC", containerFactory = "brokerC")
		public void handleItC(String body) {
		}
	}

	@Configuration
	@PropertySource("classpath:/org/springframework/amqp/rabbit/annotation/queue-annotation.properties")
	static class MultiConfig {

		static final RabbitAdmin DEFAULT_RABBIT_ADMIN = new RabbitAdmin(new SingleConnectionFactory());
		static final RabbitAdmin RABBIT_ADMIN_BROKER_B = new RabbitAdmin(new SingleConnectionFactory());
		static final RabbitAdmin RABBIT_ADMIN_BROKER_C = new RabbitAdmin(new SingleConnectionFactory());

		@Bean
		public RabbitListenerAnnotationBeanPostProcessor postProcessor() {
			MultiRabbitListenerAnnotationBeanPostProcessor postProcessor
					= new MultiRabbitListenerAnnotationBeanPostProcessor();
			postProcessor.setEndpointRegistry(rabbitListenerEndpointRegistry());
			postProcessor.setContainerFactoryBeanName("defaultContainerFactory");
			return postProcessor;
		}

		@Bean("defaultRabbitAdmin")
		public RabbitAdmin defaultRabbitAdmin() {
			return DEFAULT_RABBIT_ADMIN;
		}

		@Bean("brokerB-admin")
		public RabbitAdmin rabbitAdminBrokerB() {
			return RABBIT_ADMIN_BROKER_B;
		}

		@Bean("brokerC-admin")
		public RabbitAdmin rabbitAdminBrokerC() {
			return RABBIT_ADMIN_BROKER_C;
		}

		@Bean("defaultContainerFactory")
		public RabbitListenerContainerTestFactory defaultContainerFactory() {
			return new RabbitListenerContainerTestFactory();
		}

		@Bean("brokerB")
		public RabbitListenerContainerTestFactory containerFactoryBrokerB() {
			return new RabbitListenerContainerTestFactory();
		}

		@Bean("brokerC")
		public RabbitListenerContainerTestFactory containerFactoryBrokerC() {
			return new RabbitListenerContainerTestFactory();
		}

		@Bean
		public RabbitListenerEndpointRegistry rabbitListenerEndpointRegistry() {
			return new RabbitListenerEndpointRegistry();
		}

		@Bean
		public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
			return new PropertySourcesPlaceholderConfigurer();
		}
	}
}
