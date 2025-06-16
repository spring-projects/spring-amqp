/*
 * Copyright 2002-present the original author or authors.
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

import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;

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
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * @author Stephane Nicoll
 * @author Artem Bilan
 * @author Gary Russell
 */
public class EnableRabbitTests extends AbstractRabbitAnnotationDrivenTests {

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
		assertThatThrownBy(
				() -> new AnnotationConfigApplicationContext(EnableRabbitSampleConfig.class, FullBean.class).close())
			.isExactlyInstanceOf(BeanCreationException.class)
			.withFailMessage("'rabbitAdmin'");
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
	public void rabbitHandlerMethodFactoryConfiguration() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				EnableRabbitHandlerMethodFactoryConfig.class, ValidationBean.class);

		assertThatThrownBy(() -> testRabbitHandlerMethodFactoryConfiguration(context))
			.isExactlyInstanceOf(ListenerExecutionFailedException.class)
			.hasCauseExactlyInstanceOf(MethodArgumentNotValidException.class);
	}

	@Test
	public void unknownFactory() {
		assertThatThrownBy(
				() -> new AnnotationConfigApplicationContext(EnableRabbitSampleConfig.class, CustomBean.class).close())
			.isExactlyInstanceOf(BeanCreationException.class)
			.withFailMessage("customFactory");
	}

	@Test
	public void invalidPriorityConfiguration() {
		assertThatThrownBy(
				() -> new AnnotationConfigApplicationContext(EnableRabbitSampleConfig.class, InvalidPriorityBean.class)
					.close())
			.isExactlyInstanceOf(BeanCreationException.class)
			.withFailMessage("NotANumber");
	}

	@Test
	public void lazyComponent() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				EnableRabbitDefaultContainerFactoryConfig.class, LazyBean.class);
		RabbitListenerContainerTestFactory defaultFactory =
				context.getBean("rabbitListenerContainerFactory", RabbitListenerContainerTestFactory.class);
		assertThat(defaultFactory.getListenerContainers()).hasSize(0);

		context.getBean(LazyBean.class); // trigger lazy resolution
		assertThat(defaultFactory.getListenerContainers()).hasSize(1);
		MessageListenerTestContainer container = defaultFactory.getListenerContainers().get(0);
		assertThat(container.isStarted()).as("Should have been started " + container).isTrue();
		context.close(); // Close and stop the listeners
		assertThat(container.isStopped()).as("Should have been stopped " + container).isTrue();
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
			assertThat(messageListenerContainer.isRunning()).isFalse();
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

		@Bean
		public RabbitAdmin myAdmin() {
			return new RabbitAdmin(mock(ConnectionFactory.class));
		}

		static class Listener {

			@RabbitListener(bindings =
				@QueueBinding(value = @Queue(value = "foo", ignoreDeclarationExceptions = "true",
						declare = "false", admins = "myAdmin"),
					exchange = @Exchange(value = "bar", ignoreDeclarationExceptions = "true",
						declare = "false", admins = "myAdmin"),
					key = "baz", ignoreDeclarationExceptions = "true",
						declare = "false", admins = "myAdmin"))
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
