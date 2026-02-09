/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.client;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.aopalliance.aop.Advice;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.qpid.protonj2.client.Client;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.client.config.AmqpDefaultConfiguration;
import org.springframework.amqp.client.config.AmqpListenerEndpointRegistration;
import org.springframework.amqp.client.config.AmqpListenerEndpointRegistry;
import org.springframework.amqp.client.config.AmqpMessageListenerContainerFactory;
import org.springframework.amqp.client.config.EnableAmqp;
import org.springframework.amqp.client.config.SimpleAmqpListenerEndpoint;
import org.springframework.amqp.client.listener.AmqpMessageListenerContainer;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.aop.interceptor.DebugInterceptor;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.ErrorHandler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;

/**
 * @author Artem Bilan
 *
 * @since 4.1
 */
@SpringJUnitConfig(EnableAmqpTests.TestConfig.class)
@DirtiesContext
class EnableAmqpTests extends AbstractTestContainerTests {

	static final String TEST_QUEUE1 = "/queues/enable_amqp_queue1";

	static final String TEST_QUEUE2 = "/queues/enable_amqp_queue2";

	static final String[] QUEUE_NAMES = {
			TEST_QUEUE1,
			TEST_QUEUE2
	};

	@BeforeAll
	static void initQueues() throws IOException, InterruptedException {
		for (String queue : QUEUE_NAMES) {
			RABBITMQ.execInContainer("rabbitmqadmin", "queues", "declare", "--name", queue.replaceFirst("/queues/", ""));
		}
	}

	@Autowired
	BeanFactory beanFactory;

	@Autowired
	AmqpListenerEndpointRegistry registry;

	@Autowired
	AmqpClient amqpClient;

	@Autowired
	TaskExecutor taskExecutor;

	@Test
	void simpleEndpointBasedOnTheDefaultContainerFactory() throws InterruptedException {
		BlockingQueue<Message> receivedMessages = new LinkedBlockingQueue<>();
		MessageListener messageListener = receivedMessages::add;
		var simpleAmqpListenerEndpoint = new SimpleAmqpListenerEndpoint(messageListener, TEST_QUEUE1);

		this.registry.registerListenerEndpoint(simpleAmqpListenerEndpoint);

		// See AmqpListenerEndpointRegistry.registerListenerContainer() for generated bean name
		String containerId = messageListener.getClass().getName() + ".listenerContainer";
		var listenerContainer = this.beanFactory.getBean(containerId, AmqpMessageListenerContainer.class);

		assertThat(listenerContainer.isAutoStartup()).isFalse();
		assertThat(listenerContainer.getListenerId()).isEqualTo(containerId);
		assertThat(TestUtils.<Boolean>getPropertyValue(listenerContainer, "autoAccept")).isFalse();
		assertThat(TestUtils.<Integer>getPropertyValue(listenerContainer, "consumersPerQueue")).isEqualTo(3);
		assertThat(TestUtils.<Integer>getPropertyValue(listenerContainer, "initialCredits")).isEqualTo(55);
		assertThat(TestUtils.<String[]>getPropertyValue(listenerContainer, "queues")).containsExactly(TEST_QUEUE1);
		assertThat(TestUtils.<Advice[]>getPropertyValue(listenerContainer, "adviceChain"))
				.hasOnlyElementsOfType(DebugInterceptor.class);
		assertThatObject(TestUtils.getPropertyValue(listenerContainer, "taskExecutor")).isSameAs(this.taskExecutor);
		assertThat(TestUtils.<Duration>getPropertyValue(listenerContainer, "receiveTimeout"))
				.isEqualTo(Duration.ofSeconds(5));
		assertThat(TestUtils.<Duration>getPropertyValue(listenerContainer, "gracefulShutdownPeriod"))
				.isEqualTo(Duration.ofSeconds(36));
		assertThat(TestUtils.<ErrorHandler>getPropertyValue(listenerContainer, "errorHandler"))
				.satisfies(errorHandler ->
						assertThat(mockingDetails(errorHandler).isMock()).isTrue());

		listenerContainer.start();

		this.amqpClient.to(TEST_QUEUE1)
				.body("test_data")
				.send();

		Message message = receivedMessages.poll(10, TimeUnit.SECONDS);
		assertThat(message)
				.extracting(Message::getBody)
				.isEqualTo("test_data".getBytes());

		listenerContainer.stop();
	}

	@Test
	void simpleEndpointOverridesContainerFactoryOptions() throws Throwable {
		BlockingQueue<Message> receivedMessages = new LinkedBlockingQueue<>();
		MessageListener messageListener = receivedMessages::add;
		String containerId = "test-endpoint";
		var simpleAmqpListenerEndpoint = new SimpleAmqpListenerEndpoint(messageListener, TEST_QUEUE2);
		simpleAmqpListenerEndpoint.setId(containerId);
		simpleAmqpListenerEndpoint.setAutoStartup(true);
		simpleAmqpListenerEndpoint.setAutoAccept(true);
		simpleAmqpListenerEndpoint.setConcurrency(5);
		simpleAmqpListenerEndpoint.setInitialCredits(11);
		MethodInterceptor mockInterceptor = MethodInvocation::proceed;
		simpleAmqpListenerEndpoint.setAdviceChain(mockInterceptor);
		TaskExecutor testTaskExecutor = new SimpleAsyncTaskExecutor();
		simpleAmqpListenerEndpoint.setTaskExecutor(testTaskExecutor);
		simpleAmqpListenerEndpoint.setReceiveTimeout(Duration.ofSeconds(7));
		simpleAmqpListenerEndpoint.setGracefulShutdownPeriod(Duration.ofSeconds(17));

		this.registry.registerListenerEndpoints(new AmqpListenerEndpointRegistration(simpleAmqpListenerEndpoint));

		var listenerContainer = this.beanFactory.getBean(containerId, AmqpMessageListenerContainer.class);

		assertThat(listenerContainer.isAutoStartup()).isTrue();
		assertThat(listenerContainer.getListenerId()).isEqualTo(containerId);
		assertThat(TestUtils.<Boolean>getPropertyValue(listenerContainer, "autoAccept")).isTrue();
		assertThat(TestUtils.<Integer>getPropertyValue(listenerContainer, "consumersPerQueue")).isEqualTo(5);
		assertThat(TestUtils.<Integer>getPropertyValue(listenerContainer, "initialCredits")).isEqualTo(11);
		assertThat(TestUtils.<String[]>getPropertyValue(listenerContainer, "queues")).containsExactly(TEST_QUEUE2);
		assertThat(TestUtils.<Advice[]>getPropertyValue(listenerContainer, "adviceChain")[0]).isSameAs(mockInterceptor);
		assertThatObject(TestUtils.getPropertyValue(listenerContainer, "taskExecutor")).isSameAs(testTaskExecutor);
		assertThat(TestUtils.<Duration>getPropertyValue(listenerContainer, "receiveTimeout"))
				.isEqualTo(Duration.ofSeconds(7));
		assertThat(TestUtils.<Duration>getPropertyValue(listenerContainer, "gracefulShutdownPeriod"))
				.isEqualTo(Duration.ofSeconds(17));
		assertThat(TestUtils.<ErrorHandler>getPropertyValue(listenerContainer, "errorHandler"))
				.satisfies(errorHandler ->
						assertThat(mockingDetails(errorHandler).isMock()).isTrue());

		this.amqpClient.to(TEST_QUEUE2)
				.body("test_data2")
				.send();

		Message message = receivedMessages.poll(10, TimeUnit.SECONDS);
		assertThat(message)
				.extracting(Message::getBody)
				.isEqualTo("test_data2".getBytes());

		listenerContainer.stop();
	}

	@Test
	void noListenerContainerFactory() {
		try (var applicationContext = new AnnotationConfigApplicationContext(InvalidTestConfig.class)) {
			var endpointRegistry = applicationContext.getBean(AmqpListenerEndpointRegistry.class);
			assertThatIllegalStateException()
					.isThrownBy(() ->
							endpointRegistry.registerListenerEndpoints(
									new AmqpListenerEndpointRegistration()
											.addEndpoint(mock())))
					.withMessage("No 'AmqpMessageListenerContainerFactory' provided in the registration, " +
							"and no default listener container factory available.");
		}
	}

	@Configuration(proxyBeanMethods = false)
	@EnableAmqp
	static class TestConfig {

		@Bean
		AmqpConnectionFactory amqpConnectionFactory(Client protonClient) {
			return new SingleAmqpConnectionFactory(protonClient)
					.setPort(amqpPort());
		}

		@Bean
		AmqpClient amqpClient(AmqpConnectionFactory connectionFactory) {
			return AmqpClient.create(connectionFactory);
		}

		@Bean
		TaskExecutor taskExecutor() {
			return new SimpleAsyncTaskExecutor();
		}

		@Bean(AmqpDefaultConfiguration.DEFAULT_AMQP_LISTENER_CONTAINER_FACTORY_BEAN_NAME)
		AmqpMessageListenerContainerFactory containerFactory(AmqpConnectionFactory connectionFactory,
				TaskExecutor taskExecutor) {

			var containerFactory = new AmqpMessageListenerContainerFactory(connectionFactory);
			containerFactory.setAutoStartup(false);
			containerFactory.setAutoAccept(false);
			containerFactory.setConcurrency(3);
			containerFactory.setReceiveTimeout(Duration.ofSeconds(5));
			containerFactory.setGracefulShutdownPeriod(Duration.ofSeconds(36));
			containerFactory.setInitialCredits(55);
			containerFactory.setTaskExecutor(taskExecutor);
			containerFactory.setAdviceChain(new DebugInterceptor());
			containerFactory.setErrorHandler(mock());
			return containerFactory;
		}

	}

	@Configuration(proxyBeanMethods = false)
	@EnableAmqp
	static class InvalidTestConfig {

		@Bean
		AmqpConnectionFactory amqpConnectionFactory() {
			return mock();
		}

	}

}
