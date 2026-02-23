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

package org.springframework.amqp.client.listener;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.aopalliance.aop.Advice;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Delivery;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.amqp.client.AmqpClient;
import org.springframework.amqp.client.AmqpConnectionFactory;
import org.springframework.amqp.client.SingleAmqpConnectionFactory;
import org.springframework.amqp.client.annotation.AmqpListener;
import org.springframework.amqp.client.config.AmqpDefaultConfiguration;
import org.springframework.amqp.client.config.AmqpMessageListenerContainerFactory;
import org.springframework.amqp.client.config.EnableAmqp;
import org.springframework.amqp.client.config.MethodAmqpMessageListenerContainerFactory;
import org.springframework.amqp.core.AmqpAcknowledgment;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.listener.adapter.HandlerAdapter;
import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests;
import org.springframework.amqp.support.AmqpHeaderMapper;
import org.springframework.amqp.support.SimpleAmqpHeaderMapper;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.aop.interceptor.DebugInterceptor;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.util.AopTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;

/**
 * @author Artem Bilan
 *
 * @since 4.1
 */
@SpringJUnitConfig
@DirtiesContext
class AmqpListenerAnnotationTests extends AbstractTestContainerTests {

	static final String TEST_QUEUE1 = "/queues/listener_annotation1";

	static final String TEST_QUEUE2 = "/queues/listener_annotation2";

	static final String TEST_QUEUE3 = "/queues/listener_annotation3";

	static final String TEST_REPLY_TO = "/queues/listener_annotation_reply_to";

	static final String[] QUEUE_NAMES = {
			TEST_QUEUE1,
			TEST_QUEUE2,
			TEST_QUEUE3,
			TEST_REPLY_TO
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
	AmqpClient amqpClient;

	@Autowired
	TestConfig testConfig;

	@Test
	void simpleAmqpListenerBasedOnDefaultContainerFactoryAndDeliversAllTheArguments() throws InterruptedException {
		AmqpMessageListenerContainer listenerContainer =
				this.beanFactory.getBean(TestConfig.class.getName() + ".simpleListener.listener",
						AmqpMessageListenerContainer.class);

		assertThat(listenerContainer.isAutoStartup()).isFalse();
		assertThat(TestUtils.<Integer>getPropertyValue(listenerContainer, "consumersPerQueue")).isEqualTo(3);
		assertThat(TestUtils.<Integer>getPropertyValue(listenerContainer, "initialCredits")).isEqualTo(55);
		assertThat(TestUtils.<String[]>getPropertyValue(listenerContainer, "queues")).containsExactly(TEST_QUEUE1);
		assertThatObject(TestUtils.getPropertyValue(listenerContainer, "taskExecutor"))
				.isSameAs(this.testConfig.taskExecutor);
		assertThat(TestUtils.<Duration>getPropertyValue(listenerContainer, "receiveTimeout"))
				.isEqualTo(Duration.ofSeconds(5));
		assertThat(TestUtils.<Duration>getPropertyValue(listenerContainer, "gracefulShutdownPeriod"))
				.isEqualTo(Duration.ofSeconds(36));

		Object messageListener = listenerContainer.getMessageListener();
		assertThat(AopUtils.isAopProxy(messageListener)).isTrue();
		assertThat(AopUtils.getTargetClass(messageListener)).isEqualTo(AmqpMessagingListenerAdapter.class);
		messageListener = AopTestUtils.getTargetObject(messageListener);
		HandlerAdapter handlerAdapter = TestUtils.getPropertyValue(messageListener, "handlerAdapter");
		assertThat(handlerAdapter.getBean()).isSameAs(this.testConfig);
		assertThat(handlerAdapter.getMethod().getName()).isEqualTo("simpleListener");
		assertThatObject(TestUtils.getPropertyValue(messageListener, "errorHandler"))
				.satisfies(errorHandler ->
						assertThat(mockingDetails(errorHandler).isMock()).isTrue());

		assertThat(TestUtils.<Boolean>getPropertyValue(messageListener, "defaultRequeueRejected")).isFalse();
		assertThatObject(TestUtils.getPropertyValue(messageListener, "messagingMessageConverter.headerMapper"))
				.isSameAs(this.testConfig.headerMapper);
		assertThatObject(TestUtils.getPropertyValue(messageListener, "messagingMessageConverter.payloadConverter"))
				.isSameAs(this.testConfig.messageConverter);

		this.amqpClient.to(TEST_QUEUE1)
				.body("test_data")
				.send();

		listenerContainer.start();

		Object result = this.testConfig.results.poll(10, TimeUnit.SECONDS);

		assertThat(result)
				.asInstanceOf(InstanceOfAssertFactories.list(Object.class))
				.hasSize(8)
				.doesNotContainNull();

		listenerContainer.stop();
	}

	@Autowired
	MessageConverter jsonMessageConverter;

	@Test
	void requestReplyAndContainerFactoryOverrides() {
		var listenerContainer = this.beanFactory.getBean("monoListener", AmqpMessageListenerContainer.class);

		assertThat(listenerContainer.isAutoStartup()).isTrue();
		assertThat(TestUtils.<Boolean>getPropertyValue(listenerContainer, "autoAccept")).isFalse();
		assertThat(TestUtils.<Integer>getPropertyValue(listenerContainer, "consumersPerQueue")).isEqualTo(2);
		assertThat(TestUtils.<Integer>getPropertyValue(listenerContainer, "initialCredits")).isEqualTo(101);
		assertThat(TestUtils.<String[]>getPropertyValue(listenerContainer, "queues")).containsExactly(TEST_QUEUE2, TEST_QUEUE3);
		assertThatObject(TestUtils.getPropertyValue(listenerContainer, "taskExecutor"))
				.isSameAs(this.beanFactory.getBean("taskExecutor", TaskExecutor.class));
		assertThat(TestUtils.<Duration>getPropertyValue(listenerContainer, "receiveTimeout"))
				.isEqualTo(Duration.ofSeconds(22));
		assertThat(TestUtils.<Duration>getPropertyValue(listenerContainer, "gracefulShutdownPeriod"))
				.isEqualTo(Duration.ofMinutes(2));
		assertThat(TestUtils.<Advice[]>getPropertyValue(listenerContainer, "adviceChain")[0])
				.isSameAs(this.beanFactory.getBean("debugInterceptor", DebugInterceptor.class));

		Object messageListener = listenerContainer.getMessageListener();
		assertThat(AopUtils.isAopProxy(messageListener)).isTrue();
		assertThat(AopUtils.getTargetClass(messageListener)).isEqualTo(AmqpMessagingListenerAdapter.class);
		messageListener = AopTestUtils.getTargetObject(messageListener);
		HandlerAdapter handlerAdapter = TestUtils.getPropertyValue(messageListener, "handlerAdapter");
		assertThat(handlerAdapter.getBean()).isSameAs(this.testConfig);
		assertThat(handlerAdapter.getMethod().getName()).isEqualTo("requestReplyWithJsonAndOtherAnnotationAttributes");

		assertThat(TestUtils.<Boolean>getPropertyValue(messageListener, "defaultRequeueRejected")).isTrue();
		assertThatObject(TestUtils.getPropertyValue(messageListener, "messagingMessageConverter.headerMapper"))
				.isSameAs(this.beanFactory.getBean("headerMapper", AmqpHeaderMapper.class));
		assertThatObject(TestUtils.getPropertyValue(messageListener, "messagingMessageConverter.payloadConverter"))
				.isSameAs(this.jsonMessageConverter);

		DataIn dataIn = new DataIn("test_data");

		this.amqpClient.to(TEST_QUEUE2)
				.body(dataIn)
				.replyTo(TEST_REPLY_TO)
				.send();

		CompletableFuture<DataOut> dataOut =
				this.amqpClient.from(TEST_REPLY_TO)
						.receiveAndConvert();

		assertThat(dataOut)
				.succeedsWithin(Duration.ofSeconds(30))
				.isEqualTo(new DataOut(dataIn.data + "_out"));
	}

	@Configuration(proxyBeanMethods = false)
	@EnableAmqp
	static class TestConfig {

		@Bean
		MessageConverter jsonMessageConverter() {
			return new JacksonJsonMessageConverter();
		}

		@Bean
		AmqpClient amqpClient(AmqpConnectionFactory connectionFactory, MessageConverter jsonMessageConverter) {
			return AmqpClient.builder(connectionFactory)
					.messageConverter(jsonMessageConverter)
					.build();
		}

		@Bean
		AmqpConnectionFactory amqpConnectionFactory(Client protonClient) {
			return new SingleAmqpConnectionFactory(protonClient)
					.setPort(amqpPort());
		}

		TaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();

		AmqpHeaderMapper headerMapper = new SimpleAmqpHeaderMapper();

		MessageConverter messageConverter = new SimpleMessageConverter();

		@Bean(AmqpDefaultConfiguration.DEFAULT_AMQP_LISTENER_CONTAINER_FACTORY_BEAN_NAME)
		AmqpMessageListenerContainerFactory containerFactory(AmqpConnectionFactory connectionFactory) {
			var containerFactory = new MethodAmqpMessageListenerContainerFactory(connectionFactory);
			containerFactory.setAutoStartup(false);
			// Due to Mono return type, autoAccept is overridden to false in the target listener container
			containerFactory.setAutoAccept(false);
			containerFactory.setConcurrency(3);
			containerFactory.setReceiveTimeout(Duration.ofSeconds(5));
			containerFactory.setGracefulShutdownPeriod(Duration.ofSeconds(36));
			containerFactory.setInitialCredits(55);
			containerFactory.setTaskExecutor(this.taskExecutor);
			containerFactory.setAdviceChain(new DebugInterceptor());
			containerFactory.setListenerErrorHandler(mock());
			containerFactory.setDefaultRequeueRejected(false);
			containerFactory.setHeaderMapper(this.headerMapper);
			containerFactory.setMessageConverter(this.messageConverter);
			return containerFactory;
		}

		final BlockingQueue<Object> results = new LinkedBlockingQueue<>();

		@AmqpListener(addresses = TEST_QUEUE1)
		void simpleListener(String payload, Message message, MessageProperties messageProperties,
				org.springframework.messaging.Message<?> mesasgingMessage, MessageHeaders headers, Delivery delivery,
				AmqpAcknowledgment acknowledgment, @Header(MessageHeaders.CONTENT_TYPE) String contentType) {

			List<Object> arguments =
					List.of(payload, message, messageProperties, mesasgingMessage, headers, delivery, acknowledgment,
							contentType);

			this.results.add(arguments);

			// Would be null if autoAccept == true
			acknowledgment.acknowledge();
		}

		@Bean
		AmqpMessageListenerContainerFactory customContainerFactory(AmqpConnectionFactory connectionFactory) {
			var containerFactory = new MethodAmqpMessageListenerContainerFactory(connectionFactory);
			containerFactory.setAutoStartup(false);
			containerFactory.setAutoAccept(true);
			containerFactory.setConcurrency(5);
			containerFactory.setReceiveTimeout(Duration.ofSeconds(5));
			containerFactory.setGracefulShutdownPeriod(Duration.ofSeconds(36));
			containerFactory.setInitialCredits(55);
			containerFactory.setTaskExecutor(this.taskExecutor);
			containerFactory.setAdviceChain(new DebugInterceptor());
			containerFactory.setListenerErrorHandler(mock());
			containerFactory.setDefaultRequeueRejected(false);
			containerFactory.setHeaderMapper(this.headerMapper);
			containerFactory.setMessageConverter(this.messageConverter);
			return containerFactory;
		}

		@Bean
		TaskExecutor taskExecutor() {
			ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
			threadPoolTaskExecutor.setCorePoolSize(10);
			return threadPoolTaskExecutor;
		}

		@Bean
		AmqpHeaderMapper headerMapper() {
			return new SimpleAmqpHeaderMapper();
		}

		@Bean
		DebugInterceptor debugInterceptor() {
			return new DebugInterceptor();
		}

		@AmqpListener(
				id = "monoListener",
				addresses = TEST_QUEUE2 + "," + TEST_QUEUE3,
				concurrency = "2",
				autoStartup = "#{true}",
				autoAccept = "true",
				containerFactory = "customContainerFactory",
				receiveTimeout = "${receive.timeout:22000}",
				gracefulShutdownPeriod = "pt2m",
				initialCredits = "101",
				defaultRequeueRejected = "true",
				executor = "#{taskExecutor}",
				messageConverter = "jsonMessageConverter",
				headerMapper = "headerMapper",
				adviceChain = "debugInterceptor")
		Mono<DataOut> requestReplyWithJsonAndOtherAnnotationAttributes(DataIn dataIn) {
			DataOut dataOut = new DataOut(dataIn.data + "_out");
			return Mono.just(dataOut);
		}

	}

	record DataIn(String data) {

	}

	record DataOut(String data) {

	}

}
