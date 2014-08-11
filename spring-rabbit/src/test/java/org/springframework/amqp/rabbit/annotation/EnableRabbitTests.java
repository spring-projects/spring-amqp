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


import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.config.RabbitListenerContainerTestFactory;
import org.springframework.amqp.rabbit.config.RabbitListenerEndpointRegistrar;
import org.springframework.amqp.rabbit.config.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;

import static org.mockito.Mockito.*;

/**
 *
 * @author Stephane Nicoll
 */
public class EnableRabbitTests extends AbstractRabbitAnnotationDrivenTests {

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	@Override
	@Test
	public void sampleConfiguration() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				EnableRabbitSampleConfig.class, SampleBean.class);
		testSampleConfiguration(context);
	}

	@Override
	@Test
	public void fullConfiguration() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				EnableRabbitFullConfig.class, FullBean.class);
		testFullConfiguration(context);
	}

	@Override
	public void noRabbitAdminConfiguration() {
		thrown.expect(BeanCreationException.class);
		thrown.expectMessage("'rabbitAdmin'");
		new AnnotationConfigApplicationContext(EnableRabbitSampleConfig.class, FullBean.class);
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
				EnableRabbitSampleConfig.class, CustomBean.class);
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

}
