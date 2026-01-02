/*
 * Copyright 2025-present the original author or authors.
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

package org.springframework.amqp.rabbitmq.client.listener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import com.rabbitmq.client.amqp.Consumer;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.amqp.core.AmqpAcknowledgment;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.annotation.RabbitListenerAnnotationBeanPostProcessor;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbitmq.client.AmqpConnectionFactory;
import org.springframework.amqp.rabbitmq.client.RabbitAmqpTestBase;
import org.springframework.amqp.rabbitmq.client.config.RabbitAmqpListenerContainerFactory;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.MultiValueMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Artem Bilan
 *
 * @since 4.0
 */
@ContextConfiguration
class RabbitAmqpListenerTests extends RabbitAmqpTestBase {

	@Autowired
	Config config;

	@Autowired
	RabbitListenerEndpointRegistry rabbitListenerEndpointRegistry;

	@Autowired
	MessageConverter testMessageConverter;

	@Test
	@SuppressWarnings("unchecked")
	void verifyAllDataIsConsumedFromQ1AndQ2() throws InterruptedException {
		MessageListenerContainer testAmqpListener =
				this.rabbitListenerEndpointRegistry.getListenerContainer("testAmqpListener");

		assertThat(testAmqpListener).extracting("queueToConsumers")
				.asInstanceOf(InstanceOfAssertFactories.map(String.class, List.class))
				.hasSize(2)
				.values()
				.flatMap(list -> (List<com.rabbitmq.client.amqp.Consumer>) list)
				.hasSize(4);

		assertThat(testAmqpListener.getMessageListener())
				.isInstanceOf(RabbitAmqpMessageListenerAdapter.class)
				.extracting("messageConverter")
				.isSameAs(this.testMessageConverter);

		List<String> testDataList =
				List.of("data1", "data2", "requeue", "data4", "data5", "discard", "data7", "data8", "discard", "data10");

		Random random = new Random();

		for (String testData : testDataList) {
			this.template.convertAndSend((random.nextInt(2) == 0 ? "q1" : "q2"), testData);
		}

		assertThat(this.config.consumeIsDone.await(20, TimeUnit.SECONDS)).isTrue();

		synchronized (this.config.received) {
			assertThat(this.config.received).containsAll(testDataList);
		}

		assertThat(this.template.receive("dlq1")).succeedsWithin(20, TimeUnit.SECONDS);
		assertThat(this.template.receive("dlq1")).succeedsWithin(20, TimeUnit.SECONDS);
	}

	@Test
	@SuppressWarnings("unchecked")
	void verifyBatchConsumedAfterScheduledTimeout() {
		List<String> testDataList =
				List.of("batchData1", "batchData2", "batchData3", "batchData4", "batchData5");

		for (String testData : testDataList) {
			this.template.convertAndSend("q3", testData);
		}

		assertThat(this.config.batchReceived).succeedsWithin(20, TimeUnit.SECONDS)
				.asInstanceOf(InstanceOfAssertFactories.LIST)
				.hasSize(5)
				.containsAll(testDataList);

		assertThat(this.config.batchReceivedOnThread).startsWith("batch-consumer-scheduler-");

		MessageListenerContainer testBatchListener =
				this.rabbitListenerEndpointRegistry.getListenerContainer("testBatchListener");

		MultiValueMap<String, Consumer> queueToConsumers =
				TestUtils.getPropertyValue(testBatchListener, "queueToConsumers", MultiValueMap.class);
		Consumer consumer = queueToConsumers.get("q3").get(0);

		assertThat(consumer.unsettledMessageCount()).isEqualTo(0L);

		this.config.batchReceived = new CompletableFuture<>();

		testDataList =
				IntStream.range(6, 16)
						.boxed()
						.map(Object::toString)
						.map("batchData"::concat)
						.toList();

		for (String testData : testDataList) {
			this.template.convertAndSend("q3", testData);
		}

		assertThat(this.config.batchReceived).succeedsWithin(20, TimeUnit.SECONDS)
				.asInstanceOf(InstanceOfAssertFactories.LIST)
				.hasSize(10)
				.containsAll(testDataList);

		assertThat(this.config.batchReceivedOnThread).startsWith("dispatching-rabbitmq-amqp-");
	}

	@Test
	void verifyBasicRequestReply() {
		CompletableFuture<String> replyFuture = this.template.convertSendAndReceive("requestQueue", "test data");
		assertThat(replyFuture).succeedsWithin(20, TimeUnit.SECONDS).isEqualTo("TEST DATA");
	}

	@Test
	void verifyFutureReturnRequestReply() {
		CompletableFuture<String> replyFuture = this.template.convertSendAndReceive("requestQueue2", "TEST DATA2");
		assertThat(replyFuture).succeedsWithin(20, TimeUnit.SECONDS).isEqualTo("test data2");
	}

	@Test
	void verifyMonoReturnRequestReply() {
		CompletableFuture<String> replyFuture = this.template.convertSendAndReceive("requestQueue3", "test data3");
		assertThat(replyFuture).succeedsWithin(20, TimeUnit.SECONDS).isEqualTo("Mono test data3");
	}

	@Test
	void verifyReplyOnAnotherQueue() {
		this.template.convertAndSend("requestQueue4", "test data4");
		CompletableFuture<Object> replyFuture = this.template.receiveAndConvert("q4");
		assertThat(replyFuture).succeedsWithin(20, TimeUnit.SECONDS)
				.isEqualTo("Reply for 'test data4' via 'e1' and 'k4'");
	}

	@Configuration
	@EnableRabbit
	static class Config {

		@Bean
		Queue q1() {
			return QueueBuilder.durable("q1").deadLetterExchange("dlx1").build();
		}

		@Bean
		Queue q2() {
			return QueueBuilder.durable("q2").deadLetterExchange("dlx1").build();
		}

		@Bean
		Queue q3() {
			return new Queue("q3");
		}

		@Bean
		DirectExchange e1() {
			return new DirectExchange("e1");
		}

		@Bean
		Queue q4() {
			return new Queue("q4");
		}

		@Bean
		Binding b4() {
			return BindingBuilder.bind(q4()).to(e1()).with("k4");
		}

		@Bean
		MessageConverter testMessageConverter() {
			return new SimpleMessageConverter();
		}

		@Bean(RabbitListenerAnnotationBeanPostProcessor.DEFAULT_RABBIT_LISTENER_CONTAINER_FACTORY_BEAN_NAME)
		RabbitAmqpListenerContainerFactory rabbitAmqpListenerContainerFactory(AmqpConnectionFactory connectionFactory,
				MessageConverter testMessageConverter) {

			var rabbitAmqpListenerContainerFactory = new RabbitAmqpListenerContainerFactory(connectionFactory);
			rabbitAmqpListenerContainerFactory.setMessageConverter(testMessageConverter);
			return rabbitAmqpListenerContainerFactory;
		}

		final List<String> received = Collections.synchronizedList(new ArrayList<>());

		CountDownLatch consumeIsDone = new CountDownLatch(11);

		@RabbitListener(queues = {"q1", "q2"},
				ackMode = "#{T(org.springframework.amqp.core.AcknowledgeMode).MANUAL}",
				concurrency = "2",
				id = "testAmqpListener")
		void processQ1AndQ2Data(String data, AmqpAcknowledgment acknowledgment, Consumer.Context context) {
			try {
				if ("discard".equals(data)) {
					if (!this.received.contains(data)) {
						context.discard();
					}
					else {
						throw new MessageConversionException("Test message is rejected");
					}
				}
				else if ("requeue".equals(data) && !this.received.contains(data)) {
					acknowledgment.acknowledge(AmqpAcknowledgment.Status.REQUEUE);
				}
				else {
					acknowledgment.acknowledge();
				}
				this.received.add(data);
			}
			finally {
				this.consumeIsDone.countDown();
			}
		}

		@Bean
		ThreadPoolTaskScheduler taskScheduler() {
			ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
			threadPoolTaskScheduler.setPoolSize(2);
			threadPoolTaskScheduler.setThreadNamePrefix("batch-consumer-scheduler-");
			return threadPoolTaskScheduler;
		}

		@Bean
		RabbitAmqpListenerContainerFactory batchRabbitAmqpListenerContainerFactory(
				AmqpConnectionFactory connectionFactory, ThreadPoolTaskScheduler taskScheduler) {

			RabbitAmqpListenerContainerFactory rabbitAmqpListenerContainerFactory =
					new RabbitAmqpListenerContainerFactory(connectionFactory);
			rabbitAmqpListenerContainerFactory.setTaskScheduler(taskScheduler);
			rabbitAmqpListenerContainerFactory.setBatchSize(10);
			rabbitAmqpListenerContainerFactory.setBatchReceiveTimeout(1000L);
			return rabbitAmqpListenerContainerFactory;
		}

		CompletableFuture<List<String>> batchReceived = new CompletableFuture<>();

		volatile String batchReceivedOnThread;

		@RabbitListener(queues = "q3",
				containerFactory = "batchRabbitAmqpListenerContainerFactory",
				id = "testBatchListener")
		void processBatchFromQ3(List<String> data) {
			this.batchReceivedOnThread = Thread.currentThread().getName();
			this.batchReceived.complete(data);
		}

		@RabbitListener(queuesToDeclare = @org.springframework.amqp.rabbit.annotation.Queue("requestQueue"))
		String toUpperCaseRpc(String data) {
			return data.toUpperCase();
		}

		@RabbitListener(queuesToDeclare = @org.springframework.amqp.rabbit.annotation.Queue("requestQueue2"))
		CompletableFuture<String> toLowerCaseFutureRpc(String data) {
			return CompletableFuture.completedFuture(data)
					.thenApply(String::toLowerCase);
		}

		@RabbitListener(queuesToDeclare = @org.springframework.amqp.rabbit.annotation.Queue("requestQueue3"))
		Mono<String> monoRpc(String data) {
			return Mono.just(data)
					.map(value -> "Mono " + value);
		}

		@RabbitListener(queuesToDeclare = @org.springframework.amqp.rabbit.annotation.Queue("requestQueue4"))
		@SendTo("e1/k4")
		String replyViaSendTo(String data) {
			return "Reply for '%s' via 'e1' and 'k4'".formatted(data);
		}

	}

}
