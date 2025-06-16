/*
 * Copyright 2020-present the original author or authors.
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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.rabbitmq.client.Channel;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.amqp.core.AbstractExchange;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.rabbit.config.MessageListenerTestContainer;
import org.springframework.amqp.rabbit.config.RabbitListenerConfigUtils;
import org.springframework.amqp.rabbit.config.RabbitListenerContainerTestFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.SimpleResourceHolder;
import org.springframework.amqp.rabbit.connection.SimpleRoutingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
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
class MockMultiRabbitTests {

	@Test
	@DisplayName("Test instantiation of multiple message listeners")
	void multipleSimpleMessageListeners() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(MultiConfig.class,
				SimpleMessageListenerTestBean.class);

		Map<String, RabbitListenerContainerTestFactory> factories = context
				.getBeansOfType(RabbitListenerContainerTestFactory.class, false, false);
		Assertions.assertThat(factories).hasSize(4);

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
			listenerContainer.setReceiveTimeout(10);
			methodEndpoint.setupListenerContainer(listenerContainer);
			Assertions.assertThat(listenerContainer.getMessageListener()).isNotNull();
		});

		context.close(); // Close and stop the listeners
	}

	@Test
	@DisplayName("Test declarables matching the proper declaring admin")
	void testDeclarablesMatchProperRabbitAdmin() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(MultiConfig.class,
				AutoBindingListenerTestBeans.class);

		Map<String, RabbitListenerContainerTestFactory> factories = context
				.getBeansOfType(RabbitListenerContainerTestFactory.class, false, false);
		Assertions.assertThat(factories).hasSize(4);

		BiFunction<RabbitAdmin, Declarable, Boolean> declares = (admin, dec) -> dec.getDeclaringAdmins().size() == 1
				&& dec.getDeclaringAdmins().contains(admin.getBeanName());

		Map<String, AbstractExchange> exchanges = context.getBeansOfType(AbstractExchange.class, false, false)
				.values().stream().collect(Collectors.toMap(AbstractExchange::getName, v -> v));
		Assertions.assertThat(exchanges).hasSize(4);
		Assertions.assertThat(declares.apply(MultiConfig.DEFAULT_RABBIT_ADMIN, exchanges.get("testExchange"))).isTrue();
		Assertions.assertThat(declares.apply(MultiConfig.RABBIT_ADMIN_BROKER_B, exchanges.get("testExchangeB")))
				.isTrue();
		Assertions.assertThat(declares.apply(MultiConfig.RABBIT_ADMIN_BROKER_C, exchanges.get("testExchangeC")))
				.isTrue();
		Assertions.assertThat(declares.apply(MultiConfig.RABBIT_ADMIN_BROKER_D, exchanges.get("testExchangeD")))
				.isTrue();

		Map<String, org.springframework.amqp.core.Queue> queues = context
				.getBeansOfType(org.springframework.amqp.core.Queue.class, false, false)
				.values().stream().collect(Collectors.toMap(org.springframework.amqp.core.Queue::getName, v -> v));
		Assertions.assertThat(queues).hasSize(4);
		Assertions.assertThat(declares.apply(MultiConfig.DEFAULT_RABBIT_ADMIN, queues.get("testQueue"))).isTrue();
		Assertions.assertThat(declares.apply(MultiConfig.RABBIT_ADMIN_BROKER_B, queues.get("testQueueB"))).isTrue();
		Assertions.assertThat(declares.apply(MultiConfig.RABBIT_ADMIN_BROKER_C, queues.get("testQueueC"))).isTrue();
		Assertions.assertThat(declares.apply(MultiConfig.RABBIT_ADMIN_BROKER_D, queues.get("testQueueD"))).isTrue();

		Map<String, Binding> bindings = context.getBeansOfType(Binding.class, false, false)
				.values().stream().collect(Collectors.toMap(Binding::getRoutingKey, v -> v));
		Assertions.assertThat(bindings).hasSize(4);
		Assertions.assertThat(declares.apply(MultiConfig.DEFAULT_RABBIT_ADMIN, bindings.get("testKey"))).isTrue();
		Assertions.assertThat(declares.apply(MultiConfig.RABBIT_ADMIN_BROKER_B, bindings.get("testKeyB"))).isTrue();
		Assertions.assertThat(declares.apply(MultiConfig.RABBIT_ADMIN_BROKER_C, bindings.get("testKeyC"))).isTrue();
		Assertions.assertThat(declares.apply(MultiConfig.RABBIT_ADMIN_BROKER_D, bindings.get("testKeyD"))).isTrue();

		context.close(); // Close and stop the listeners
	}

	@Test
	@DisplayName("Test stand-alone declarable not associated to any declaring admin")
	void testStandAloneDeclarablesNotEnhancedWithSpecificDeclaringAdmin() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(MultiConfig.class,
				StandAloneDeclarablesConfig.class, AutoBindingListenerTestBeans.class);

		Declarable exchange = context.getBean(StandAloneDeclarablesConfig.EXCHANGE, AbstractExchange.class);
		Assertions.assertThat(exchange.getDeclaringAdmins()).isEmpty();

		Declarable queue = context.getBean(StandAloneDeclarablesConfig.QUEUE,
				org.springframework.amqp.core.Queue.class);
		Assertions.assertThat(queue.getDeclaringAdmins()).isEmpty();

		Declarable binding = context.getBean(StandAloneDeclarablesConfig.BINDING, Binding.class);
		Assertions.assertThat(binding.getDeclaringAdmins()).isEmpty();

		context.close(); // Close and stop the listeners
	}

	@Test
	@DisplayName("Test creation of connections at the proper brokers")
	void testCreationOfConnections() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(MultiConfig.class,
				AutoBindingListenerTestBeans.class);

		final RabbitTemplate rabbitTemplate = new RabbitTemplate(MultiConfig.ROUTING_CONNECTION_FACTORY);

		Mockito.verify(MultiConfig.DEFAULT_CONNECTION_FACTORY, Mockito.never()).createConnection();
		Mockito.verify(MultiConfig.DEFAULT_CONNECTION, Mockito.never()).createChannel(false);
		rabbitTemplate.convertAndSend("messageDefaultBroker");
		Mockito.verify(MultiConfig.DEFAULT_CONNECTION_FACTORY).createConnection();
		Mockito.verify(MultiConfig.DEFAULT_CONNECTION).createChannel(false);

		Mockito.verify(MultiConfig.CONNECTION_FACTORY_BROKER_B, Mockito.never()).createConnection();
		Mockito.verify(MultiConfig.CONNECTION_BROKER_B, Mockito.never()).createChannel(false);
		SimpleResourceHolder.bind(MultiConfig.ROUTING_CONNECTION_FACTORY, "brokerB");
		rabbitTemplate.convertAndSend("messageToBrokerB");
		SimpleResourceHolder.unbind(MultiConfig.ROUTING_CONNECTION_FACTORY);
		Mockito.verify(MultiConfig.CONNECTION_FACTORY_BROKER_B).createConnection();
		Mockito.verify(MultiConfig.CONNECTION_BROKER_B).createChannel(false);

		Mockito.verify(MultiConfig.CONNECTION_FACTORY_BROKER_C, Mockito.never()).createConnection();
		Mockito.verify(MultiConfig.CONNECTION_BROKER_C, Mockito.never()).createChannel(false);
		SimpleResourceHolder.bind(MultiConfig.ROUTING_CONNECTION_FACTORY, "brokerC");
		rabbitTemplate.convertAndSend("messageToBrokerC");
		SimpleResourceHolder.unbind(MultiConfig.ROUTING_CONNECTION_FACTORY);
		Mockito.verify(MultiConfig.CONNECTION_FACTORY_BROKER_C).createConnection();
		Mockito.verify(MultiConfig.CONNECTION_BROKER_C).createChannel(false);

		Mockito.verify(MultiConfig.CONNECTION_FACTORY_BROKER_D, Mockito.never()).createConnection();
		Mockito.verify(MultiConfig.CONNECTION_BROKER_D, Mockito.never()).createChannel(false);
		SimpleResourceHolder.bind(MultiConfig.ROUTING_CONNECTION_FACTORY, "brokerD");
		rabbitTemplate.convertAndSend("messageToBrokerD");
		SimpleResourceHolder.unbind(MultiConfig.ROUTING_CONNECTION_FACTORY);
		Mockito.verify(MultiConfig.CONNECTION_FACTORY_BROKER_D).createConnection();
		Mockito.verify(MultiConfig.CONNECTION_BROKER_D).createChannel(false);

		context.close(); // Close and stop the listeners
	}



	@Test
	@DisplayName("Test assignment of RabbitAdmin in the endpoint registry")
	void testAssignmentOfRabbitAdminInTheEndpointRegistry() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(MultiConfig.class,
				AutoBindingListenerTestBeans.class);

		final RabbitListenerEndpointRegistry registry = context.getBean(RabbitListenerEndpointRegistry.class);
		final Collection<MessageListenerContainer> listenerContainers = registry.getListenerContainers();

		Assertions.assertThat(listenerContainers).hasSize(4);
		listenerContainers.forEach(container -> {
			Assertions.assertThat(container).isInstanceOf(MessageListenerTestContainer.class);
			final MessageListenerTestContainer refContainer = (MessageListenerTestContainer) container;
			final RabbitListenerEndpoint endpoint = refContainer.getEndpoint();
			Assertions.assertThat(endpoint).isInstanceOf(MethodRabbitListenerEndpoint.class);
			final MethodRabbitListenerEndpoint refEndpoint = (MethodRabbitListenerEndpoint) endpoint;
			Assertions.assertThat(refEndpoint.getAdmin()).isNotNull();
		});

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

		@RabbitListener(containerFactory = "${broker-name:brokerD}", bindings = @QueueBinding(
				exchange = @Exchange("testExchangeD"),
				value = @Queue("testQueueD"),
				key = "testKeyD"))
		public void handleItD(String body) {
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

		@RabbitListener(queues = "testQueueD", containerFactory = "${broker-name:brokerD}")
		public void handleItD(String body) {
		}
	}

	@Configuration
	@PropertySource("classpath:/org/springframework/amqp/rabbit/annotation/queue-annotation.properties")
	static class MultiConfig {

		static final SimpleRoutingConnectionFactory ROUTING_CONNECTION_FACTORY = new SimpleRoutingConnectionFactory();
		static final ConnectionFactory DEFAULT_CONNECTION_FACTORY = Mockito.mock(ConnectionFactory.class);
		static final ConnectionFactory CONNECTION_FACTORY_BROKER_B = Mockito.mock(ConnectionFactory.class);
		static final ConnectionFactory CONNECTION_FACTORY_BROKER_C = Mockito.mock(ConnectionFactory.class);
		static final ConnectionFactory CONNECTION_FACTORY_BROKER_D = Mockito.mock(ConnectionFactory.class);

		static final Connection DEFAULT_CONNECTION = Mockito.mock(Connection.class);
		static final Connection CONNECTION_BROKER_B = Mockito.mock(Connection.class);
		static final Connection CONNECTION_BROKER_C = Mockito.mock(Connection.class);
		static final Connection CONNECTION_BROKER_D = Mockito.mock(Connection.class);

		static final Channel DEFAULT_CHANNEL = Mockito.mock(Channel.class);
		static final Channel CHANNEL_BROKER_B = Mockito.mock(Channel.class);
		static final Channel CHANNEL_BROKER_C = Mockito.mock(Channel.class);
		static final Channel CHANNEL_BROKER_D = Mockito.mock(Channel.class);

		static {
			final Map<Object, ConnectionFactory> targetConnectionFactories = new HashMap<>();
			targetConnectionFactories.put("brokerB", CONNECTION_FACTORY_BROKER_B);
			targetConnectionFactories.put("brokerC", CONNECTION_FACTORY_BROKER_C);
			targetConnectionFactories.put("brokerD", CONNECTION_FACTORY_BROKER_D);
			ROUTING_CONNECTION_FACTORY.setDefaultTargetConnectionFactory(DEFAULT_CONNECTION_FACTORY);
			ROUTING_CONNECTION_FACTORY.setTargetConnectionFactories(targetConnectionFactories);

			Mockito.when(DEFAULT_CONNECTION_FACTORY.createConnection()).thenReturn(DEFAULT_CONNECTION);
			Mockito.when(CONNECTION_FACTORY_BROKER_B.createConnection()).thenReturn(CONNECTION_BROKER_B);
			Mockito.when(CONNECTION_FACTORY_BROKER_C.createConnection()).thenReturn(CONNECTION_BROKER_C);
			Mockito.when(CONNECTION_FACTORY_BROKER_D.createConnection()).thenReturn(CONNECTION_BROKER_D);

			Mockito.when(DEFAULT_CONNECTION.createChannel(false)).thenReturn(DEFAULT_CHANNEL);
			Mockito.when(CONNECTION_BROKER_B.createChannel(false)).thenReturn(CHANNEL_BROKER_B);
			Mockito.when(CONNECTION_BROKER_C.createChannel(false)).thenReturn(CHANNEL_BROKER_C);
			Mockito.when(CONNECTION_BROKER_D.createChannel(false)).thenReturn(CHANNEL_BROKER_D);
		}

		static final RabbitAdmin DEFAULT_RABBIT_ADMIN = new RabbitAdmin(DEFAULT_CONNECTION_FACTORY);
		static final RabbitAdmin RABBIT_ADMIN_BROKER_B = new RabbitAdmin(CONNECTION_FACTORY_BROKER_B);
		static final RabbitAdmin RABBIT_ADMIN_BROKER_C = new RabbitAdmin(CONNECTION_FACTORY_BROKER_C);
		static final RabbitAdmin RABBIT_ADMIN_BROKER_D = new RabbitAdmin(CONNECTION_FACTORY_BROKER_D);

		@Bean
		public RabbitListenerAnnotationBeanPostProcessor postProcessor() {
			MultiRabbitListenerAnnotationBeanPostProcessor postProcessor
					= new MultiRabbitListenerAnnotationBeanPostProcessor();
			postProcessor.setEndpointRegistry(rabbitListenerEndpointRegistry());
			postProcessor.setContainerFactoryBeanName("defaultContainerFactory");
			return postProcessor;
		}

		@Bean(RabbitListenerConfigUtils.RABBIT_ADMIN_BEAN_NAME)
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

		@Bean("brokerD-admin")
		public RabbitAdmin rabbitAdminBrokerD() {
			return RABBIT_ADMIN_BROKER_D;
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

		@Bean("brokerD")
		public RabbitListenerContainerTestFactory containerFactoryBrokerD() {
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

	@Configuration
	static class StandAloneDeclarablesConfig {

		static final String EXCHANGE = "standAloneExchange";
		static final String QUEUE = "standAloneQueue";
		static final String BINDING = "standAloneBinding";

		@Bean(name = EXCHANGE)
		public AbstractExchange exchange() {
			return new DirectExchange(EXCHANGE);
		}

		@Bean(name = QUEUE)
		public org.springframework.amqp.core.Queue queue() {
			return new org.springframework.amqp.core.Queue(QUEUE);
		}

		@Bean(name = BINDING)
		public Binding binding() {
			return new Binding(QUEUE, Binding.DestinationType.QUEUE, EXCHANGE, BINDING, null);
		}
	}
}
