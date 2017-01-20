/*
 * Copyright 2002-2017 the original author or authors.
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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.config.MessageListenerTestContainer;
import org.springframework.amqp.rabbit.config.RabbitListenerContainerTestFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

/**
 * @author Stephane Nicoll
 * @author Artem Bilan
 * @author Gary Russell
 */
public class EnableRabbitTests extends AbstractRabbitAnnotationDrivenTests {

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	@Override
	@Test
	public void sampleConfiguration() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				EnableRabbitSampleConfig.class, SampleBean.class);
		testSampleConfiguration(context, 2);
	}

	@Override
	@Test
	public void fullConfiguration() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				EnableRabbitFullConfig.class, FullBean.class);
		testFullConfiguration(context);
	}

	@Override
	@Test
	public void fullConfigurableConfiguration() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				EnableRabbitFullConfigurableConfig.class, FullConfigurableBean.class);
		testFullConfiguration(context);
	}

	@Override
	public void noRabbitAdminConfiguration() {
		thrown.expect(BeanCreationException.class);
		thrown.expectMessage("'rabbitAdmin'");
		new AnnotationConfigApplicationContext(EnableRabbitSampleConfig.class, FullBean.class).close();
	}

	@Override
	@Test
	public void customConfiguration() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				EnableRabbitCustomConfig.class, CustomBean.class);
		testCustomConfiguration(context);
	}

	@Override
	@Test
	public void explicitContainerFactory() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				EnableRabbitCustomContainerFactoryConfig.class, DefaultBean.class);
		testExplicitContainerFactoryConfiguration(context);
	}

	@Override
	@Test
	public void defaultContainerFactory() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				EnableRabbitDefaultContainerFactoryConfig.class, DefaultBean.class);
		testDefaultContainerFactoryConfiguration(context);
	}

	@Override
	@Test
	public void rabbitHandlerMethodFactoryConfiguration() throws Exception {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				EnableRabbitHandlerMethodFactoryConfig.class, ValidationBean.class);

		thrown.expect(ListenerExecutionFailedException.class);
		thrown.expectCause(Is.<MethodArgumentNotValidException>isA(MethodArgumentNotValidException.class));
		testRabbitHandlerMethodFactoryConfiguration(context);
	}

	@Test
	public void unknownFactory() {
		thrown.expect(BeanCreationException.class);
		thrown.expectMessage("customFactory"); // Not found
		new AnnotationConfigApplicationContext(
				EnableRabbitSampleConfig.class, CustomBean.class).close();
	}

	@Test
	public void invalidPriorityConfiguration() {
		thrown.expect(BeanCreationException.class);
		thrown.expectMessage("NotANumber"); // Invalid number
		new AnnotationConfigApplicationContext(
				EnableRabbitSampleConfig.class, InvalidPriorityBean.class).close();
	}

	@Test
	public void lazyComponent() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				EnableRabbitDefaultContainerFactoryConfig.class, LazyBean.class);
		RabbitListenerContainerTestFactory defaultFactory =
				context.getBean("rabbitListenerContainerFactory", RabbitListenerContainerTestFactory.class);
		assertEquals(0, defaultFactory.getListenerContainers().size());

		context.getBean(LazyBean.class); // trigger lazy resolution
		assertEquals(1, defaultFactory.getListenerContainers().size());
		MessageListenerTestContainer container = defaultFactory.getListenerContainers().get(0);
		assertTrue("Should have been started " + container, container.isStarted());
		context.close(); // Close and stop the listeners
		assertTrue("Should have been stopped " + container, container.isStopped());
	}

	@Override
	@Test
	public void rabbitListeners() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				EnableRabbitDefaultContainerFactoryConfig.class,
				RabbitListenersBean.class,
				ClassLevelListenersBean.class);
		testRabbitListenerRepeatable(context);
	}

	@Test
	public void testProperShutdownOnException() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				ProperShutdownConfig.class,
				RabbitListenersBean.class,
				ClassLevelListenersBean.class);
		RabbitListenerEndpointRegistry listenerEndpointRegistry = context.getBean(RabbitListenerEndpointRegistry.class);

		// Previously this takes 30 seconds to finish (see DefaultLifecycleProcessor#timeoutPerShutdownPhase)
		// And not all containers has been stopped from the RabbitListenerEndpointRegistry
		context.close();
		for (MessageListenerContainer messageListenerContainer : listenerEndpointRegistry.getListenerContainers()) {
			assertFalse(messageListenerContainer.isRunning());
		}
	}

	@EnableRabbit
	@Configuration
	static class EnableRabbitSampleConfig {

		@Bean
		public RabbitListenerContainerTestFactory rabbitListenerContainerFactory() {
			return new RabbitListenerContainerTestFactory();
		}

		@Bean
		public RabbitListenerContainerTestFactory simpleFactory() {
			return new RabbitListenerContainerTestFactory();
		}

		@Bean
		public Listener listener() {
			return new Listener();
		}

		static class Listener {

			@RabbitListener(bindings =
				@QueueBinding(value = @Queue(value = "foo", ignoreDeclarationExceptions = "true"),
					exchange = @Exchange(value = "bar", ignoreDeclarationExceptions = "true"),
					key = "baz", ignoreDeclarationExceptions = "true"))
			public void handle(String foo) {
				// empty
			}

		}

	}

	@EnableRabbit
	@Configuration
	static class EnableRabbitFullConfig {

		@Bean
		public RabbitListenerContainerTestFactory simpleFactory() {
			return new RabbitListenerContainerTestFactory();
		}

		@Bean
		public RabbitAdmin rabbitAdmin() {
			return mock(RabbitAdmin.class);
		}
	}

	@EnableRabbit
	@Configuration
	@PropertySource("classpath:/org/springframework/amqp/rabbit/annotation/rabbit-listener.properties")
	static class EnableRabbitFullConfigurableConfig {

		@Bean
		public RabbitListenerContainerTestFactory simpleFactory() {
			return new RabbitListenerContainerTestFactory();
		}

		@Bean
		public RabbitAdmin rabbitAdmin() {
			return mock(RabbitAdmin.class);
		}

		@Bean
		public PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
			return new PropertySourcesPlaceholderConfigurer();
		}
	}

	@Configuration
	@EnableRabbit
	static class EnableRabbitCustomConfig implements RabbitListenerConfigurer {

		@Override
		public void configureRabbitListeners(RabbitListenerEndpointRegistrar registrar) {
			registrar.setEndpointRegistry(customRegistry());

			// Also register a custom endpoint
			SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
			endpoint.setId("myCustomEndpointId");
			endpoint.setQueueNames("myQueue");
			endpoint.setMessageListener(simpleMessageListener());
			registrar.registerEndpoint(endpoint);
		}

		@Bean
		public RabbitListenerContainerTestFactory rabbitListenerContainerFactory() {
			return new RabbitListenerContainerTestFactory();
		}

		@Bean
		public RabbitListenerEndpointRegistry customRegistry() {
			return new RabbitListenerEndpointRegistry();
		}

		@Bean
		public RabbitListenerContainerTestFactory customFactory() {
			return new RabbitListenerContainerTestFactory();
		}

		@Bean
		public MessageListener simpleMessageListener() {
			return new MessageListenerAdapter();
		}
	}

	@Configuration
	@EnableRabbit
	static class EnableRabbitCustomContainerFactoryConfig implements RabbitListenerConfigurer {

		@Override
		public void configureRabbitListeners(RabbitListenerEndpointRegistrar registrar) {
			registrar.setContainerFactory(simpleFactory());
		}

		@Bean
		public RabbitListenerContainerTestFactory simpleFactory() {
			return new RabbitListenerContainerTestFactory();
		}
	}

	@Configuration
	@EnableRabbit
	static class EnableRabbitDefaultContainerFactoryConfig {

		@Bean
		public RabbitListenerContainerTestFactory rabbitListenerContainerFactory() {
			return new RabbitListenerContainerTestFactory();
		}
	}

	@Configuration
	@EnableRabbit
	static class EnableRabbitHandlerMethodFactoryConfig implements RabbitListenerConfigurer {

		@Override
		public void configureRabbitListeners(RabbitListenerEndpointRegistrar registrar) {
			registrar.setMessageHandlerMethodFactory(customMessageHandlerMethodFactory());
		}

		@Bean
		public MessageHandlerMethodFactory customMessageHandlerMethodFactory() {
			DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();
			factory.setValidator(new TestValidator());
			return factory;
		}

		@Bean
		public RabbitListenerContainerTestFactory defaultFactory() {
			return new RabbitListenerContainerTestFactory();
		}

	}

	@Configuration
	@EnableRabbit
	static class ProperShutdownConfig {

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory containerFactory = new SimpleRabbitListenerContainerFactory() {

				@Override
				protected SimpleMessageListenerContainer createContainerInstance() {
					SimpleMessageListenerContainer listenerContainer = spy(super.createContainerInstance());

					willThrow(RuntimeException.class)
							.given(listenerContainer)
							.shutdown();

					return listenerContainer;
				}

			};

			ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

			Connection connection = mock(Connection.class);

			given(connection.createChannel(anyBoolean()))
					.willReturn(mock(Channel.class));

			given(connectionFactory.createConnection())
					.willReturn(connection);

			containerFactory.setConnectionFactory(connectionFactory);
			return containerFactory;
		}

	}

	@Component
	static class InvalidPriorityBean {

		@RabbitListener(queues = "myQueue", priority = "NotANumber")
		public void customHandle(String msg) {
		}

	}

	@Component
	@Lazy
	static class LazyBean {

		@RabbitListener(queues = "myQueue")
		public void handle(String msg) {
		}

	}

}
